/*
 * Copyright 2015-2016 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/genam.h"
#include "access/relscan.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "tcop/utility.h"


#include "zdb_interface.h"
#include "util/zdbutils.h"


typedef struct {
    char current_dbname[NAMEDATALEN];
} ZDBWorkerState;

static ZDBWorkerState *state = NULL;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup  = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static double worker_naptime = 1;

int zdb_vacuum_xact_index(ZDBIndexDescriptor *desc);
void zdbxactvac_init(void);

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void worker_spi_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;

    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno          = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void worker_spi_sighup(SIGNAL_ARGS) {
    int save_errno = errno;

    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno          = save_errno;
}

static bool can_vacuum(ItemPointer itemptr, void *state) {
    HTAB   *htab  = (HTAB *) state;
    uint64 tid_64 = ItemPointerToUint64(itemptr);
    bool   found;

    hash_search(htab, &tid_64, HASH_FIND, &found);
    return found;
}

int zdb_vacuum_xact_index(ZDBIndexDescriptor *desc) {
    TransactionId        oldestXmin;
    TransactionId        lastXid    = InvalidTransactionId;
    TransactionId        currentXid = GetCurrentTransactionId();
    BufferAccessStrategy strategy   = GetAccessStrategy(BAS_VACUUM);
    Snapshot             snapshot   = GetActiveSnapshot();
    IndexScanDesc        scanDesc;
    Relation             heapRel    = NULL;
    Relation             xactRel;
    ItemPointer          tid;
    int                  killed     = 0, tuples = 0;
    bool                 found;
    HASHCTL              ctl;
    HTAB                 *htab;

    MemSet(&ctl, 0, sizeof(HASHCTL));
    ctl.keysize   = sizeof(uint64);
    ctl.entrysize = sizeof(uint64);
    ctl.hash      = tag_hash;
    htab = hash_create("killable tids", 2048, &ctl, HASH_ELEM | HASH_FUNCTION);

    if (ConditionalLockRelationOid(desc->heapRelid, ShareUpdateExclusiveLock)) {
        int  maxkilled    = 1024;
        ItemPointer killableTids = palloc(maxkilled * sizeof(ItemPointerData)); /* array of killed tids */;

        heapRel = heap_open(desc->heapRelid, NoLock);

#if (PG_VERSION_NUM >= 90400)
        oldestXmin = GetOldestXmin(heapRel, true);
#else
        oldestXmin = GetOldestXmin(false, true);
#endif

        if (visibilitymap_count(heapRel) != RelationGetNumberOfBlocks(heapRel)) {
            xactRel  = index_open(desc->xactRelId, RowExclusiveLock);
            scanDesc = index_beginscan(heapRel, xactRel, SnapshotAny, 0, 0);
            scanDesc->xs_want_itup         = true;
            scanDesc->ignore_killed_tuples = true;

            index_rescan(scanDesc, NULL, 0, NULL, 0);
            while ((tid = index_getnext_tid(scanDesc, ForwardScanDirection)) != NULL) {
                uint64        convertedxid;
                TransactionId xid;
                bool          is_insert;
                bool          isnull;
                bool          canKill = false;

                convertedxid = (uint64) DatumGetInt64(index_getattr(scanDesc->xs_itup, 1, scanDesc->xs_itupdesc, &isnull));
                is_insert    = DatumGetBool(index_getattr(scanDesc->xs_itup, 2, scanDesc->xs_itupdesc, &isnull));
                xid          = (TransactionId) (convertedxid);

                if (!is_insert && TransactionIdPrecedesOrEquals(xid, currentXid) && xid != lastXid && !is_invisible_xid(snapshot, xid)) {
                    BlockNumber  blockno = ItemPointerGetBlockNumber(tid);
                    OffsetNumber offnum  = ItemPointerGetOffsetNumber(tid);
                    Buffer       buffer;
                    Page         page;
                    ItemId       itemid;

                    buffer = ReadBufferExtended(heapRel, MAIN_FORKNUM, blockno, RBM_NORMAL, strategy);
                    if (ConditionalLockBufferForCleanup(buffer)) {
                        page   = BufferGetPage(buffer);
                        itemid = PageGetItemId(page, offnum);

                        if (!ItemIdIsUsed(itemid)) {
                            /* noop */
                        } else if (ItemIdIsRedirected(itemid)) {
                            /* noop */
                        } else if (ItemIdIsDead(itemid)) {
                            canKill = true;
                            lastXid = InvalidTransactionId;
                        } else {
                            HeapTupleData tuple;

                            ItemPointerCopy(tid, &(tuple.t_self));
                            tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
                            tuple.t_len  = ItemIdGetLength(itemid);

#if (PG_VERSION_NUM >= 90400)
                            switch (HeapTupleSatisfiesVacuum(&tuple, oldestXmin, buffer)) {
#else
                            switch (HeapTupleSatisfiesVacuum(tuple.t_data, oldestXmin, buffer)) {
#endif
                                case HEAPTUPLE_DEAD:
                                    if (TransactionIdDidCommit(xid)) {
                                        canKill = true;
                                        lastXid = InvalidTransactionId;
                                    }
                                    break;

                                case HEAPTUPLE_LIVE:
                                    if (TransactionIdDidAbort(xid)) {
                                        uint64 tid_64 = ItemPointerToUint64(tid);
                                        hash_search(htab, &tid_64, HASH_ENTER, &found);
                                        lastXid = InvalidTransactionId;
                                    }
                                    break;

                                default:
                                    lastXid = xid;
                                    break;
                            }
                        }
                        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                    }
                    ReleaseBuffer(buffer);

                    if (canKill) {
                        uint64 tid_64 = ItemPointerToUint64(tid);

                        if (killed >= maxkilled) {
                            /* grow our killedTids array */
                            maxkilled *= 2;
                            killableTids = repalloc(killableTids, maxkilled * sizeof(ItemPointerData));
                        }

                        ItemPointerCopy(tid, &killableTids[killed]);
                        hash_search(htab, &tid_64, HASH_ENTER, &found);
                        killed++;
                    }
                }

                tuples++;
            }
            index_endscan(scanDesc);

            if (hash_get_num_entries(htab) > 0) {
                IndexVacuumInfo *info;

                desc->implementation->bulkDelete(desc, killableTids, killed);

                info = palloc(sizeof(IndexVacuumInfo));
                info->analyze_only    = false;
                info->estimated_count = true;
                info->index           = xactRel;
                info->num_heap_tuples = tuples;
                info->strategy        = strategy;
                info->message_level   = INFO;

                index_bulk_delete(info, NULL, can_vacuum, htab);
            }
            index_close(xactRel, RowExclusiveLock);

            if (IsBackgroundWorker)
                elog(LOG, "[zombodb xact vacuum stats:%s] heap=%s, killed=%d, tuples=%d", desc->databaseName, desc->tableName, killed, tuples);
        }
        heap_close(heapRel, ShareUpdateExclusiveLock);
    }

    return killed;
}


static int process_database(char *dbname) {
    Oid *indexes;
    int i, many;
    int killed = 0;

    pgstat_report_activity(STATE_RUNNING, "[zombodb] processing");

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    indexes = find_all_zdb_indexes(&many);

    for (i = 0; i < many; i++) {
        ZDBIndexDescriptor *desc = zdb_alloc_index_descriptor_by_index_oid(indexes[i]);

        if (!desc->isShadow && OidIsValid(desc->heapRelid) && OidIsValid(desc->xactRelId)) {
            MemoryContext cxt     = AllocSetContextCreate(CurrentMemoryContext, "zombodb vacuum index", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
            MemoryContext currcxt = MemoryContextSwitchTo(cxt);

            killed += zdb_vacuum_xact_index(desc);

            MemoryContextSwitchTo(currcxt);
            MemoryContextDelete(cxt);
        }

        zdb_free_index_descriptor(desc);
    }

    PopActiveSnapshot();
    CommitTransactionCommand();

    pgstat_report_activity(STATE_IDLE, NULL);

    return killed;
}

static char *next_database(char *current) {
    StringInfoData sql;
    char           *rc = NULL;

    initStringInfo(&sql);

    SPI_connect();

    appendStringInfo(&sql, "SELECT datname FROM pg_database WHERE datname NOT IN ('template0', 'template1') AND datallowconn = true");
    if (current != NULL && strlen(current) > 0)
        appendStringInfo(&sql, " AND datname > '%s'", current);
    appendStringInfo(&sql, " ORDER BY datname LIMIT 1");

    if (SPI_exec(sql.data, 1) == SPI_OK_SELECT) {
        if (SPI_processed == 0) {
            /* no more databases after the current, so start over at the top */
            return next_database(NULL);
        } else {
            rc = MemoryContextStrdup(TopMemoryContext, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
        }
    }

    SPI_finish();

    return rc;
}

static void set_next_database(void) {
    char *nextdb;

    /* start a transaction */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    /* find the next database to process */
    nextdb = next_database(state->current_dbname);
    if (nextdb != NULL)
        strncpy(state->current_dbname, nextdb, NAMEDATALEN);
    else
        state->current_dbname[0] = '\0';

    /* finish our transaction */
    PopActiveSnapshot();
    CommitTransactionCommand();
}

static void zdb_worker_main(Datum main_arg) {
    char *currentdb = NULL;
    bool found;
    int loops = 1;

    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGHUP, worker_spi_sighup);
    pqsignal(SIGTERM, worker_spi_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    state = ShmemInitStruct("zombodb worker state", sizeof(ZDBWorkerState), &found);
    if (!found)
        memset(state, 0, sizeof(ZDBWorkerState));
    LWLockRelease(AddinShmemInitLock);

    if (state->current_dbname[0] == '\0') {
        /* first time through (or no current database to process), so find one in template1 */
        BackgroundWorkerInitializeConnection("template1", NULL);
    } else {
        /* Connect to database */
        BackgroundWorkerInitializeConnection(state->current_dbname, NULL);
        currentdb = state->current_dbname;
    }

    while (!got_sigterm) {
        int rc;
        int killed = 0;

        /*
         * Background workers mustn't call usleep() or any direct equivalent:
         * instead, they may wait on their process latch, which sleeps as
         * necessary, but is awakened if postmaster dies.  That way the
         * background process goes away immediately in an emergency.
         */
        rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, (long) (worker_naptime * 1000L));
        ResetLatch(&MyProc->procLatch);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        /*
         * In case of a SIGHUP, just reload the configuration.
         */
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* figure out where we should go next */
        set_next_database();

        /* and process any ZDB indexes we might find in the currently-connected database */
        if (currentdb != NULL) {
            killed = process_database(currentdb);
        }

        /*
         * If we didn't kill any tuples, or if we've max'd out our loop count for
         * this database, then exit the process
         */
        if (killed == 0 || loops == 25) {
            /*
             * Because we need to connect do a different database on the next "loop"
             * get out and let postgres restart us
             */

            proc_exit(0);
        }

        loops++;
    }
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void zdbxactvac_init(void) {
    BackgroundWorker worker;

    /* get the configuration */
    DefineCustomRealVariable("zombodb.xact_naptime", "Duration between each check (in seconds).", NULL, &worker_naptime, 1, 0, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);

    /* set up common data for all our workers */
    snprintf(worker.bgw_name, BGW_MAXLEN, "zombodb background worker");
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 0;
    worker.bgw_main         = zdb_worker_main;
    worker.bgw_main_arg     = PointerGetDatum(0);
    RegisterBackgroundWorker(&worker);
}
