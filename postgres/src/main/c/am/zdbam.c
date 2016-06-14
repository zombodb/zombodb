/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
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

#define ZDBSEQSCAN_INCLUDE_DEFINITIONS

#include "access/heapam_xlog.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "commands/event_trigger.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/relation.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tqual.h"

#include "zdb_interface.h"
#include "zdbops.h"
#include "zdbam.h"
#include "zdbseqscan.h"


PG_FUNCTION_INFO_V1(zdbbuild);
PG_FUNCTION_INFO_V1(zdbbuildempty);
PG_FUNCTION_INFO_V1(zdbinsert);
PG_FUNCTION_INFO_V1(zdbbeginscan);
PG_FUNCTION_INFO_V1(zdbgettuple);
PG_FUNCTION_INFO_V1(zdbrescan);
PG_FUNCTION_INFO_V1(zdbendscan);
PG_FUNCTION_INFO_V1(zdbmarkpos);
PG_FUNCTION_INFO_V1(zdbrestpos);
PG_FUNCTION_INFO_V1(zdbbulkdelete);
PG_FUNCTION_INFO_V1(zdbvacuumcleanup);
PG_FUNCTION_INFO_V1(zdboptions);
PG_FUNCTION_INFO_V1(zdbtupledeletedtrigger);
PG_FUNCTION_INFO_V1(zdbeventtrigger);
PG_FUNCTION_INFO_V1(zdbcostestimate);

PG_FUNCTION_INFO_V1(zdb_num_hits);

/* Working state for zdbbuild and its callback */
typedef struct {
    bool     isUnique;
    bool     haveDead;
    Relation heapRel;

    double indtuples;

    ZDBIndexDescriptor *desc;
} ZDBBuildState;

typedef struct {
    ZDBIndexDescriptor *indexDescriptor;
    uint64             nhits;
    uint64             currhit;
    ZDBSearchResponse  *hits;
    char               **queries;
    int                nqueries;
} ZDBScanState;

typedef struct {
    IndexBulkDeleteCallback callback;
    void                    *callback_state;
    List                    *bulkDeleteList;
    int                     many;
} ZDBBulkDeleteState;

static void zdbbuildCallback(Relation indexRel, HeapTuple htup, Datum *values, bool *isnull, bool tupleIsAlive, void *state);

static TransactionId lastxid = 0;

static List *usedIndexesList     = NULL;
static List *indexesInsertedList = NULL;

/* For tracking/pushing changed tuples */
static ExecutorStart_hook_type prev_ExecutorStartHook = NULL;
static ExecutorEnd_hook_type   prev_ExecutorEndHook   = NULL;

/* for tracking Executor nesting depth */
static ExecutorRun_hook_type    prev_ExecutorRunHook    = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinishHook = NULL;
static int                      executorDepth           = 0;

static int64 numHitsFound = -1;

static void pushCurrentQuery(QueryDesc *queryDesc) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    CURRENT_QUERY_STACK = lcons(queryDesc, CURRENT_QUERY_STACK);
    MemoryContextSwitchTo(oldContext);
}

static void popCurrentQuery() {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    CURRENT_QUERY_STACK = list_delete_first(CURRENT_QUERY_STACK);
    MemoryContextSwitchTo(oldContext);
}

static ZDBIndexDescriptor *alloc_index_descriptor(Relation indexRel, bool forInsert) {
    MemoryContext      oldContext = MemoryContextSwitchTo(TopTransactionContext);
    ListCell           *lc;
    ZDBIndexDescriptor *desc      = NULL;

    foreach (lc, usedIndexesList) {
        ZDBIndexDescriptor *tmp = lfirst(lc);

        if (tmp->indexRelid == RelationGetRelid(indexRel)) {
            desc = tmp;
            break;
        }
    }

    if (desc == NULL) {
        desc            = zdb_alloc_index_descriptor(indexRel);
        usedIndexesList = lappend(usedIndexesList, desc);
    }

    if (desc->xactRelId == InvalidOid) {
        desc->xactRelId = get_relation_oid(desc->schemaName, desc->xactRelName);
    }

    if (forInsert && !desc->isShadow)
        indexesInsertedList = list_append_unique(indexesInsertedList, desc);

    MemoryContextSwitchTo(oldContext);
    return desc;
}

static void xact_complete_cleanup(XactEvent event) {
    List     *usedIndexes = usedIndexesList;
    ListCell *lc;

    /* free up our static vars on xact finish */
    usedIndexesList           = NULL;
    indexesInsertedList       = NULL;
    CURRENT_QUERY_STACK       = NULL;
    executorDepth             = 0;
    numHitsFound              = -1;

    zdb_reset_scores();
    zdb_sequential_scan_support_cleanup();
    zdb_transaction_finish();

    /* notify each index we used that the xact is over */
    foreach(lc, usedIndexes) {
        ZDBIndexDescriptor *desc = lfirst(lc);

        desc->implementation->transactionFinish(desc, event == XACT_EVENT_COMMIT ? ZDB_TRANSACTION_COMMITTED
                                                                                 : ZDB_TRANSACTION_ABORTED);
    }
    interface_transaction_cleanup();
}

static void zdbam_xact_callback(XactEvent event, void *arg) {
    switch (event) {
        case XACT_EVENT_PRE_PREPARE:
        case XACT_EVENT_PREPARE:
            elog(ERROR, "zombodb doesn't support prepared transactions");
            break;

        case XACT_EVENT_COMMIT: {
            if (zdb_batch_mode_guc) {
                ListCell *lc;

                /* finish the batch insert (and refresh) for each index into which new records were inserted */
                foreach (lc, indexesInsertedList) {
                    ZDBIndexDescriptor *desc = lfirst(lc);

                    desc->implementation->batchInsertFinish(desc);
                }
            }

        }
            break;
        case XACT_EVENT_ABORT:
            break;

        default:
            break;
    }

    switch (event) {
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_ABORT:
            /* cleanup, the xact is over */
            xact_complete_cleanup(event);
            break;
        default:
            break;
    }
}

static void zdb_executor_start_hook(QueryDesc *queryDesc, int eflags) {
    if (prev_ExecutorStartHook == zdb_executor_start_hook)
        elog(ERROR, "zdb_executor_start_hook: Somehow prev_ExecutorStartHook was set to zdb_executor_start_hook");

    pushCurrentQuery(queryDesc);

    if (prev_ExecutorStartHook)
        prev_ExecutorStartHook(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

static void zdb_executor_end_hook(QueryDesc *queryDesc) {
    if (executorDepth == 0) {
        if (!zdb_batch_mode_guc) {
            ListCell *lc;

            /* finish the batch insert (and refresh) for each index into which new records were inserted */
            foreach (lc, indexesInsertedList) {
                ZDBIndexDescriptor *desc = lfirst(lc);
                desc->implementation->batchInsertFinish(desc);
            }
        }

        /* make sure to cleanup seqscan globals too */
        zdb_sequential_scan_support_cleanup();
    }

    if (prev_ExecutorEndHook == zdb_executor_end_hook)
        elog(ERROR, "zdb_executor_end_hook: Somehow prev_ExecutorEndHook was set to zdb_executor_end_hook");

    popCurrentQuery();

    if (prev_ExecutorEndHook)
        prev_ExecutorEndHook(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

static void zdb_executor_run_hook(QueryDesc *queryDesc, ScanDirection direction, long count) {
    executorDepth++;
    PG_TRY();
            {
                pushCurrentQuery(queryDesc);

                if (prev_ExecutorRunHook)
                    prev_ExecutorRunHook(queryDesc, direction, count);
                else
                    standard_ExecutorRun(queryDesc, direction, count);
                executorDepth--;
            }
        PG_CATCH();
            {
                executorDepth--;
                PG_RE_THROW();
            }
    PG_END_TRY();
}

static void zdb_executor_finish_hook(QueryDesc *queryDesc) {
    executorDepth++;
    PG_TRY();
            {
                popCurrentQuery();

                if (prev_ExecutorFinishHook)
                    prev_ExecutorFinishHook(queryDesc);
                else
                    standard_ExecutorFinish(queryDesc);
                executorDepth--;
            }
        PG_CATCH();
            {
                executorDepth--;
                PG_RE_THROW();
            }
    PG_END_TRY();
}

void zdbam_init(void) {
    if (prev_ExecutorStartHook == zdb_executor_start_hook)
        elog(ERROR, "zdbam_init:  Unable to initialize ZomboDB.  ExecutorStartHook already assigned");
    else if (prev_ExecutorEndHook == zdb_executor_end_hook)
        elog(ERROR, "zdbam_init:  Unable to initialize ZomboDB.  ExecutorEndHook already assigned");

    zdb_index_init();

    prev_ExecutorStartHook = ExecutorStart_hook;
    prev_ExecutorEndHook   = ExecutorEnd_hook;
    ExecutorStart_hook     = zdb_executor_start_hook;
    ExecutorEnd_hook       = zdb_executor_end_hook;

    prev_ExecutorRunHook    = ExecutorRun_hook;
    prev_ExecutorFinishHook = ExecutorFinish_hook;
    ExecutorRun_hook        = zdb_executor_run_hook;
    ExecutorFinish_hook     = zdb_executor_finish_hook;

    RegisterXactCallback(zdbam_xact_callback, NULL);
}

void zdbam_fini(void) {
    if (prev_ExecutorEndHook == zdb_executor_end_hook)
        elog(ERROR, "zdbam_fini: Somehow prev_ExecutorEndHook was set to zdb_executor_end_hook");

    ExecutorStart_hook = prev_ExecutorStartHook;
    ExecutorEnd_hook   = prev_ExecutorEndHook;

    ExecutorRun_hook    = prev_ExecutorRunHook;
    ExecutorFinish_hook = prev_ExecutorFinishHook;

    UnregisterXactCallback(zdbam_xact_callback, NULL);
}

/*
 *  zdbbuild() -- build a new zombodb index.
 */
Datum zdbbuild(PG_FUNCTION_ARGS) {
    Relation         heapRel    = (Relation) PG_GETARG_POINTER(0);
    Relation         indexRel   = (Relation) PG_GETARG_POINTER(1);
    IndexInfo        *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
    TupleDesc        tupdesc    = RelationGetDescr(indexRel);
    IndexBuildResult *result;
    double           reltuples  = 0.0;
    ZDBBuildState    buildstate;
    Datum            PROPERTIES = CStringGetTextDatum("properties");
    Datum            mappingDatum;
    Datum            propertiesDatum;
    char             *properties;

    if (heapRel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
        elog(ERROR, "ZomoboDB indexes not supported on unlogged tables");

    if (!ZDBIndexOptionsGetUrl(indexRel) && !ZDBIndexOptionsGetShadow(indexRel))
        elog(ERROR, "Must set the 'url' or 'shadow' index option");

    if (tupdesc->natts != 2)
        elog(ERROR, "Incorrect number of attributes on index %s", RelationGetRelationName(indexRel));

    switch (tupdesc->attrs[1]->atttypid) {
        case JSONOID:
            break;

        default:
            elog(ERROR, "Unsupported second index column type: %d", tupdesc->attrs[1]->atttypid);
            break;
    }

    buildstate.isUnique    = indexInfo->ii_Unique;
    buildstate.haveDead    = false;
    buildstate.heapRel     = heapRel;
    buildstate.indtuples   = 0;
    buildstate.desc        = alloc_index_descriptor(indexRel, false);
    buildstate.desc->logit = true;

    if (!buildstate.desc->isShadow) {
        /* drop the existing index */
        buildstate.desc->implementation->dropIndex(buildstate.desc);

        /* create a new, empty index */
        mappingDatum    = make_es_mapping(heapRel->rd_id, heapRel->rd_att, false);
        propertiesDatum = DirectFunctionCall2(json_object_field, mappingDatum, PROPERTIES);
        properties      = TextDatumGetCString(propertiesDatum);

        buildstate.desc->implementation->createNewIndex(buildstate.desc, ZDBIndexOptionsGetNumberOfShards(indexRel), properties);

        /* do the heap scan */
        reltuples = IndexBuildHeapScan(heapRel, indexRel, indexInfo, true, zdbbuildCallback, (void *) &buildstate);

        /* signal that the batch inserts have stopped */
        buildstate.desc->implementation->batchInsertFinish(buildstate.desc);

        /* reset the settings to reasonable values for production use */
        buildstate.desc->implementation->finalizeNewIndex(buildstate.desc);

		if (heapRel->rd_rel->relkind != 'm')
		{
            StringInfo triggerSQL;

			/* put a trigger on the table to forward UPDATEs and DELETEs into our code for ES xact synchronization */
			SPI_connect();

			triggerSQL = makeStringInfo();
			appendStringInfo(triggerSQL, "SELECT * FROM pg_trigger WHERE tgname = 'zzzzdb_tuple_sync_for_%d_using_%d'", RelationGetRelid(heapRel), RelationGetRelid(indexRel));
			if (SPI_execute(triggerSQL->data, true, 0) == SPI_OK_SELECT && SPI_processed == 0)
			{
				resetStringInfo(triggerSQL);
				appendStringInfo(triggerSQL,
						"CREATE TRIGGER zzzzdb_tuple_sync_for_%d_using_%d"
								"       BEFORE UPDATE OR DELETE ON \"%s\".\"%s\" "
								"       FOR EACH ROW EXECUTE PROCEDURE zdbtupledeletedtrigger('%d');"
								"UPDATE pg_trigger SET tgisinternal = true WHERE tgname = 'zzzzdb_tuple_sync_for_%d_using_%d';"
								"SELECT oid "
								"       FROM pg_trigger "
								"       WHERE tgname = 'zzzzdb_tuple_sync_for_%d_using_%d'",
						RelationGetRelid(heapRel), RelationGetRelid(indexRel), buildstate.desc->schemaName, buildstate.desc->tableName, RelationGetRelid(indexRel),  /* CREATE TRIGGER args */
						RelationGetRelid(heapRel), RelationGetRelid(indexRel), /* UPDATE pg_trigger args */
						RelationGetRelid(heapRel), RelationGetRelid(indexRel) /* SELECT FROM pg_trigger args */
				);

				if (SPI_execute(triggerSQL->data, false, 0) == SPI_OK_SELECT && SPI_processed == 1) {
                    define_dependency(RelationRelationId, RelationGetRelid(indexRel), TriggerRelationId, (Oid) atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1)), DEPENDENCY_INTERNAL);
				}
				else {
					elog(ERROR, "Cannot create trigger");
				}
			}

			pfree(triggerSQL->data);
			pfree(triggerSQL);

			SPI_finish();
		}

        if (buildstate.desc->xactRelId == InvalidOid) {
            StringInfo sb = makeStringInfo();

            SPI_connect();

            appendStringInfo(sb, "CREATE INDEX %s ON %s.%s ((null::int8), (null::boolean)) WHERE false;", buildstate.desc->xactRelName, get_namespace_name(RelationGetNamespace(heapRel)), RelationGetRelationName(heapRel));

            elog(LOG, "[zombodb] Creating xact index for %s: %s", RelationGetRelationName(indexRel), sb->data);
            if (SPI_exec(sb->data, 0) != SPI_OK_UTILITY)
                elog(ERROR, "Problem creating zombodb xact index");

            buildstate.desc->xactRelId = get_relation_oid(buildstate.desc->schemaName, buildstate.desc->xactRelName);
            define_dependency(RelationRelationId, RelationGetRelid(indexRel), RelationRelationId, buildstate.desc->xactRelId, DEPENDENCY_INTERNAL);
            define_dependency(RelationRelationId, buildstate.desc->xactRelId, RelationRelationId, RelationGetRelid(indexRel), DEPENDENCY_NORMAL);

            SPI_finish();
        }

        pfree(DatumGetPointer(mappingDatum));
        pfree(DatumGetPointer(propertiesDatum));
        pfree(DatumGetPointer(PROPERTIES));
        pfree(properties);

        /*
         * If we are reindexing a pre-existing index, it is critical to send out a
         * relcache invalidation SI message to ensure all backends re-read the
         * index metapage.  We expect that the caller will ensure that happens
         * (typically as a side effect of updating index stats, but it must happen
         * even if the stats don't change!)
         */
    }

    /*
     * Return statistics
     */
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

    result->heap_tuples  = reltuples;
    result->index_tuples = buildstate.indtuples;

    PG_RETURN_POINTER(result);
}

/*
 * Per-tuple callback from IndexBuildHeapScan
 */
static void zdbbuildCallback(Relation indexRel, HeapTuple htup, Datum *values, bool *isnull, bool tupleIsAlive, void *state) {
    ZDBBuildState      *buildstate = (ZDBBuildState *) state;
    ZDBIndexDescriptor *desc       = buildstate->desc;

    desc->implementation->batchInsertRow(desc, &htup->t_self, DatumGetTextP(values[1]), HeapTupleHeaderGetXmin(htup->t_data));

    buildstate->indtuples++;
}

/*
 *  zdbinsert() -- insert an index tuple into a zombodb.
 */
Datum zdbinsert(PG_FUNCTION_ARGS) {
    Relation           indexRel             = (Relation) PG_GETARG_POINTER(0);
    Datum              *values              = (Datum *) PG_GETARG_POINTER(1);
//    bool	   *isnull = (bool *) PG_GETARG_POINTER(2);
    ItemPointer        ht_ctid              = (ItemPointer) PG_GETARG_POINTER(3);
    Relation           heapRel              = (Relation) PG_GETARG_POINTER(4);
//    IndexUniqueCheck checkUnique = (IndexUniqueCheck) PG_GETARG_INT32(5);
    TransactionId      currentTransactionId = GetCurrentTransactionId();
    ZDBIndexDescriptor *desc;

    desc = alloc_index_descriptor(indexRel, true);
    if (desc->isShadow)
        PG_RETURN_BOOL(false);

    if (lastxid != currentTransactionId) {
        Relation xactRel;
        Datum datum[2];
        static bool isnull[2] = {false, false};

        datum[0] = Int64GetDatum(convert_xid(currentTransactionId));
        datum[1] = true;

        /*
         * If we haven't already done so for this transaction, insert a single
         * tuple into our xact index to record this transaction id so concurrent/future
         * scans will exclude it.
         */
        xactRel = relation_open(desc->xactRelId, AccessShareLock);
        index_insert(xactRel, datum, isnull, ht_ctid, heapRel, UNIQUE_CHECK_NO);
        relation_close(xactRel, AccessShareLock);

        lastxid = currentTransactionId;
    }

    desc->implementation->batchInsertRow(desc, ht_ctid, DatumGetTextP(values[1]), currentTransactionId);

    PG_RETURN_BOOL(true);
}

/*
 *	zdbbuildempty() -- build an empty zombodb index in the initialization fork
 */
Datum zdbbuildempty(PG_FUNCTION_ARGS) {
//    Relation	index = (Relation) PG_GETARG_POINTER(0);

    PG_RETURN_VOID();
}

Datum zdb_num_hits(PG_FUNCTION_ARGS) {
    PG_RETURN_INT64(numHitsFound);
}

static void setup_scan(IndexScanDesc scan) {
    ZDBScanState       *scanstate = (ZDBScanState *) scan->opaque;
    ZDBIndexDescriptor *desc      = alloc_index_descriptor(scan->indexRelation, false);
    char               **queries;
    int                i;

    if (scanstate->hits)
        scanstate->indexDescriptor->implementation->freeSearchResponse(scanstate->hits);

    if (scanstate->queries) {
        for (i = 0; i < scanstate->nqueries; i++)
            pfree(scanstate->queries[i]);
        pfree(scanstate->queries);
    }

    queries = palloc0(scan->numberOfKeys * sizeof(char *));
    for (i  = 0; i < scan->numberOfKeys; i++) {
        ScanKey current = &scan->keyData[i];
        Datum   d       = current->sk_argument;

        if (current->sk_subtype != TEXTOID)
            elog(ERROR, "Only types of 'text' are supported for searching");

        if (DatumGetPointer(d) == NULL)
            queries[i] = pstrdup("null");
        else
            queries[i] = TextDatumGetCString(d);
    }

    scanstate->queries         = queries;
    scanstate->nqueries        = scan->numberOfKeys;
    scanstate->indexDescriptor = desc;
    scanstate->hits            = desc->implementation->searchIndex(desc, queries, scan->numberOfKeys, &scanstate->nhits);
    scanstate->currhit         = 0;

    numHitsFound = scanstate->hits->total_hits;
}

/*
 *  zdbgettuple() -- Get the next tuple in the scan.
 */
Datum zdbgettuple(PG_FUNCTION_ARGS) {
    IndexScanDesc scan       = (IndexScanDesc) PG_GETARG_POINTER(0);
    ZDBScanState  *scanstate = (ZDBScanState *) scan->opaque;
//    ScanDirection dir = (ScanDirection) PG_GETARG_INT32(1);
    bool          haveMore;

    /* zdbtree indexes are never lossy */
    scan->xs_recheck = false;

    haveMore = scanstate->currhit < scanstate->nhits;
    if (haveMore) {
        ZDBScore score;

        set_item_pointer(scanstate->hits, scanstate->currhit, &scan->xs_ctup.t_self, &score);
        scanstate->currhit++;

        zdb_record_score(RelationGetRelid(scan->indexRelation), &scan->xs_ctup.t_self, score);
    }

    PG_RETURN_BOOL(haveMore);
}

/*
 *  zdbbeginscan() -- start a scan on a zombodb index
 */
Datum zdbbeginscan(PG_FUNCTION_ARGS) {
    Relation      rel       = (Relation) PG_GETARG_POINTER(0);
    int           nkeys     = PG_GETARG_INT32(1);
    int           norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan;

    /* no order by operators allowed */
    Assert(norderbys == 0);

    /* get the scan */
    scan = RelationGetIndexScan(rel, nkeys, norderbys);

    scan->opaque = (ZDBScanState *) palloc0(sizeof(ZDBScanState));

    PG_RETURN_POINTER(scan);
}

/*
 *  zdbrescan() -- rescan an index relation
 */
Datum zdbrescan(PG_FUNCTION_ARGS) {
    IndexScanDesc scan       = (IndexScanDesc) PG_GETARG_POINTER(0);
    ScanKey       scankey    = (ScanKey) PG_GETARG_POINTER(1);
    ZDBScanState  *scanstate = (ZDBScanState *) scan->opaque;
    bool          changed    = false;

    if (scankey && scan->numberOfKeys > 0) {
        if (scanstate->queries) {
            /*
             * we've already called setup_scan() so
             * lets see if our scankeys have changed
             */

            if (scan->numberOfKeys == scanstate->nqueries) {
                int i;
                for (i = 0; i < scan->numberOfKeys; i++) {
                    ScanKey current = &scankey[i];
                    Datum   d       = current->sk_argument;
                    char    *newKey;

                    if (current->sk_subtype != TEXTOID)
                        elog(ERROR, "Only types of 'text' are supported for searching");

                    newKey = TextDatumGetCString(d);

                    if (strcmp(newKey, scanstate->queries[i]) != 0)
                        changed = true;

                    pfree(newKey);
                    if (changed)
                        break;
                }
            } else {
                changed = true;
            }
        }
    }

    if (changed || scanstate->queries == NULL) {
        memmove(scan->keyData, scankey, scan->numberOfKeys * sizeof(ScanKeyData));

        setup_scan(scan);
    } else {
        scanstate->currhit = 0;
    }

    PG_RETURN_VOID();
}

/*
 *  zdbendscan() -- close down a scan
 */
Datum zdbendscan(PG_FUNCTION_ARGS) {
    IndexScanDesc scan       = (IndexScanDesc) PG_GETARG_POINTER(0);
    ZDBScanState  *scanstate = (ZDBScanState *) scan->opaque;

    if (scanstate) {
        if (scanstate->hits) {
            scanstate->indexDescriptor->implementation->freeSearchResponse(scanstate->hits);
            scanstate->hits = NULL;
        }

        if (scanstate->queries) {
            int i;
            for (i = 0; i < scanstate->nqueries; i++)
                pfree(scanstate->queries[i]);
            pfree(scanstate->queries);
        }

        pfree(scanstate);
        scan->opaque = NULL;
    }
    PG_RETURN_VOID();
}

/*
 *  zdbmarkpos() -- save current scan position
 */
Datum zdbmarkpos(PG_FUNCTION_ARGS) {
//    IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);

    // TODO:  remember where we are in the current scan
    elog(NOTICE, "zdbmarkpos()");

    PG_RETURN_VOID();
}

/*
 *  zdbrestrpos() -- restore scan to last saved position
 */
Datum zdbrestrpos(PG_FUNCTION_ARGS) {
//    IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);

    // TODO:  take us back to where we last markpos()'d
    elog(NOTICE, "zdbrestrpos()");

    PG_RETURN_VOID();
}

static void delete_xids_from_xact_index(Relation heapRel, Relation xactRel, TransactionId oldestXmin, ZDBIndexDescriptor *desc) {
    IndexScanDesc scanDesc;
    ItemPointer   tid;
    StringInfo    xids      = makeStringInfo();
    uint64        lastxid = InvalidTransactionId;

    /*
     * scan our xact index and build a unique list (a json-formatted string of xids) of xids it contains
     * that are at least before the GetOldestXmin.  These should be completed xids, either committed or aborted
     */
    scanDesc = index_beginscan(heapRel, xactRel, SnapshotAny, 0, 0);
    scanDesc->xs_want_itup = true;

    index_rescan(scanDesc, NULL, 0, NULL, 0);
    while ((tid = index_getnext_tid(scanDesc, ForwardScanDirection)) != NULL) {
        uint64        convertedxid;
        TransactionId xid;
        bool          isnull;

        convertedxid = (uint64) DatumGetInt64(index_getattr(scanDesc->xs_itup, 1, scanDesc->xs_itupdesc, &isnull));
        xid          = (TransactionId) (convertedxid);

        if (lastxid != convertedxid && TransactionIdPrecedesOrEquals(xid, oldestXmin)) {
            if (TransactionIdDidAbort(xid)) {
                if (xids->len == 0)
                    appendStringInfoChar(xids, '[');
                else if (xids->len > 1)
                    appendStringInfoChar(xids, ',');
                appendStringInfo(xids, "%lu", convertedxid);
                lastxid = convertedxid;
            }
        }
    }

    /* if we have some xids from above... */
    if (xids->len > 0) {
        uint32 nxids;
        uint64* dead_xids;

        /*
         * ... then find the ones that no longer exist in Elasticsearch.  This means
         * previous vacuum runs managed to fully clean up the individual rows
         */
        appendStringInfoChar(xids, ']');
        dead_xids = desc->implementation->vacuumSupport(desc, xids->data, &nxids);

        if (nxids > 0) {
            HASHCTL ctl;
            HTAB *htab;
            uint32 i;
            bool found;

            /* convert our list of dead xids into a hashtable for fast lookup */
            MemSet(&ctl, 0, sizeof(HASHCTL));
            ctl.keysize   = sizeof(uint64);
            ctl.entrysize = sizeof(uint64);
            htab = hash_create("dead xids", nxids, &ctl, HASH_ELEM);
            for (i = 0; i < nxids; i++)
                hash_search(htab, &dead_xids[i], HASH_ENTER, &found);

            /*
             * and for any that are no longer in ES, we'll 'kill_prior_tuple' on them
             * to get them out of our xact index so future "invisibility map" scans
             * won't try to exclude things that no longer exist
             */
            index_rescan(scanDesc, NULL, 0, NULL, 0);
            while (index_getnext_tid(scanDesc, ForwardScanDirection) != NULL) {
                uint64 convertedxid;
                bool   isnull;

                convertedxid = (uint64) DatumGetInt64(index_getattr(scanDesc->xs_itup, 1, scanDesc->xs_itupdesc, &isnull));

                hash_search(htab, &convertedxid, HASH_FIND, &found);
                if (found) {
                    scanDesc->kill_prior_tuple = true;
                }
            }
        }
    }

    index_endscan(scanDesc);
}


/*
 * lifted from various Postgres versions of vacuumlazy.c
 *
 * We do this so that vacuuming our remote Elasticsearch index can
 * look directly at LVRelStats.dead_tuples instead of walking through
 * the index and asking about each and every tuple.
 */
#if (PG_VERSION_NUM < 90400)
typedef struct LVRelStats
{
    /* hasindex = true means two-pass strategy; false means one-pass */
    bool		hasindex;
    /* Overall statistics about rel */
    BlockNumber old_rel_pages;	/* previous value of pg_class.relpages */
    BlockNumber rel_pages;		/* total number of pages */
    BlockNumber scanned_pages;	/* number of pages we examined */
    double		scanned_tuples; /* counts only tuples on scanned pages */
    double		old_rel_tuples; /* previous value of pg_class.reltuples */
    double		new_rel_tuples; /* new estimated total # of tuples */
    BlockNumber pages_removed;
    double		tuples_deleted;
    BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
    /* List of TIDs of tuples we intend to delete */
    /* NB: this list is ordered by TID address */
    int			num_dead_tuples;	/* current # of entries */
    int			max_dead_tuples;	/* # slots allocated in array */
    ItemPointer dead_tuples;	/* array of ItemPointerData */
    int			num_index_scans;
    TransactionId latestRemovedXid;
    bool		lock_waiter_detected;
} LVRelStats;
#elif (PG_VERSION_NUM < 90500)
typedef struct LVRelStats
{
    /* hasindex = true means two-pass strategy; false means one-pass */
    bool		hasindex;
    /* Overall statistics about rel */
    BlockNumber old_rel_pages;	/* previous value of pg_class.relpages */
    BlockNumber rel_pages;		/* total number of pages */
    BlockNumber scanned_pages;	/* number of pages we examined */
    double		scanned_tuples; /* counts only tuples on scanned pages */
    double		old_rel_tuples; /* previous value of pg_class.reltuples */
    double		new_rel_tuples; /* new estimated total # of tuples */
    double		new_dead_tuples;	/* new estimated total # of dead tuples */
    BlockNumber pages_removed;
    double		tuples_deleted;
    BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
    /* List of TIDs of tuples we intend to delete */
    /* NB: this list is ordered by TID address */
    int			num_dead_tuples;	/* current # of entries */
    int			max_dead_tuples;	/* # slots allocated in array */
    ItemPointer dead_tuples;	/* array of ItemPointerData */
    int			num_index_scans;
    TransactionId latestRemovedXid;
    bool		lock_waiter_detected;
} LVRelStats;
#elif (PG_VERSION_NUM < 90600)
typedef struct LVRelStats
{
	/* hasindex = true means two-pass strategy; false means one-pass */
	bool		hasindex;
	/* Overall statistics about rel */
	BlockNumber old_rel_pages;	/* previous value of pg_class.relpages */
	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber scanned_pages;	/* number of pages we examined */
	BlockNumber pinskipped_pages;		/* # of pages we skipped due to a pin */
	double		scanned_tuples; /* counts only tuples on scanned pages */
	double		old_rel_tuples; /* previous value of pg_class.reltuples */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	BlockNumber pages_removed;
	double		tuples_deleted;
	BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
	/* List of TIDs of tuples we intend to delete */
	/* NB: this list is ordered by TID address */
	int			num_dead_tuples;	/* current # of entries */
	int			max_dead_tuples;	/* # slots allocated in array */
	ItemPointer dead_tuples;	/* array of ItemPointerData */
	int			num_index_scans;
	TransactionId latestRemovedXid;
	bool		lock_waiter_detected;
} LVRelStats;
#endif

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum zdbbulkdelete(PG_FUNCTION_ARGS) {
    IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
//    IndexBulkDeleteCallback callback        = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
    void                    *callback_state = (void *) PG_GETARG_POINTER(3);
    Relation                indexRel        = info->index;
    Relation                heapRel;
    Relation                xactRel;
    ZDBIndexDescriptor      *desc;
    TransactionId oldestXmin = GetOldestXmin(false, false);
    LVRelStats *relstats;
    struct timeval tv1, tv2;

    gettimeofday(&tv1, NULL);

    /* allocate stats if first time through, else re-use existing struct */
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    desc = alloc_index_descriptor(indexRel, false);
    if (desc->isShadow)
        PG_RETURN_POINTER(stats);

    relstats = (LVRelStats *) callback_state; /* XXX:  cast to our copy/paste of LVRelStats! */
    heapRel  = RelationIdGetRelation(desc->heapRelid);

    if (relstats->num_dead_tuples > 0) {
        desc->implementation->bulkDelete(desc, relstats->dead_tuples, relstats->num_dead_tuples);

        xactRel = relation_open(desc->xactRelId, AccessShareLock);
        delete_xids_from_xact_index(heapRel, xactRel, oldestXmin, desc);
        relation_close(xactRel, AccessShareLock);
    }

    gettimeofday(&tv2, NULL);
    elog(LOG, "[zombodb vacuum status] heap=%s, total=%d, ttl=%fs", RelationGetRelationName(heapRel), relstats->num_dead_tuples, TO_SECONDS(tv1, tv2));
    RelationClose(heapRel);

    stats->tuples_removed = relstats->num_dead_tuples;
    PG_RETURN_POINTER(stats);
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum zdbvacuumcleanup(PG_FUNCTION_ARGS) {
    IndexVacuumInfo       *info  = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);

    /* No-op in ANALYZE ONLY mode */
    if (info->analyze_only)
        PG_RETURN_POINTER(stats);

    /*
     * If zdbbulkdelete was called, we need not do anything, just return the
     * stats from the latest zdbbulkdelete call.  If it wasn't called, we must
     * still do a pass over the index, to recycle any newly-recyclable pages
     * and to obtain index statistics.
     *
     * Since we aren't going to actually delete any leaf items, there's no
     * need to go through all the vacuum-cycle-ID pushups.
     */
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    /* Finally, vacuum the FSM */
    IndexFreeSpaceMapVacuum(info->index);

    /*
     * It's quite possible for us to be fooled by concurrent page splits into
     * double-counting some index tuples, so disbelieve any total that exceeds
     * the underlying heap's count ... if we know that accurately.  Otherwise
     * this might just make matters worse.
     */
    if (!info->estimated_count) {
        if (stats->num_index_tuples > info->num_heap_tuples)
            stats->num_index_tuples = info->num_heap_tuples;
    }

    PG_RETURN_POINTER(stats);
}

Datum zdboptions(PG_FUNCTION_ARGS) {
    Datum                         reloptions = PG_GETARG_DATUM(0);
    bool                          validate   = PG_GETARG_BOOL(1);
    relopt_value                  *options;
    ZDBIndexOptions               *rdopts;
    int                           numoptions;
    static const relopt_parse_elt tab[]      = {{"url",                  RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, urlValueOffset)},
                                                {"shadow",               RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, shadowValueOffset)},
                                                {"options",              RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, optionsValueOffset)},
                                                {"preference",           RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, preferenceValueOffset)},
                                                {"refresh_interval",     RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, refreshIntervalOffset)},
                                                {"shards",               RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, shards)},
                                                {"replicas",             RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, replicas)},
                                                {"ignore_visibility",    RELOPT_TYPE_BOOL,   offsetof(ZDBIndexOptions, ignoreVisibility)},
                                                {"bulk_concurrency",     RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, bulk_concurrency)},
                                                {"batch_size",           RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, batch_size)},
                                                {"field_lists",          RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, fieldListsValueOffset)},
                                                {"always_resolve_joins", RELOPT_TYPE_BOOL,   offsetof(ZDBIndexOptions, alwaysResolveJoins)},};

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_ZDB, &numoptions);

    /* if none set, we're done */
    if (numoptions == 0)
        PG_RETURN_NULL();

    rdopts = allocateReloptStruct(sizeof(ZDBIndexOptions), options, numoptions);

    fillRelOptions((void *) rdopts, sizeof(ZDBIndexOptions), options, numoptions, validate, tab, lengthof(tab));

    pfree(options);

    PG_RETURN_BYTEA_P(rdopts);
}

Datum zdbeventtrigger(PG_FUNCTION_ARGS) {
    EventTriggerData *trigdata;

    if (!CALLED_AS_EVENT_TRIGGER(fcinfo))  /* internal error */
        elog(ERROR, "not fired by event trigger manager");

    trigdata = (EventTriggerData *) fcinfo->context;

    if (trigdata->parsetree->type == T_AlterTableStmt) {
        AlterTableStmt *stmt   = (AlterTableStmt *) trigdata->parsetree;
        Relation       heapRel = heap_openrv(stmt->relation, AccessShareLock);
        Oid            *indexOids;
        int            many;

        indexOids = findZDBIndexes(RelationGetRelid(heapRel), &many);
        if (many > 0 && indexOids != NULL) {
            int i;
            for (i = 0; i < many; i++) {
                Relation           indexRel;
                ZDBIndexDescriptor *desc;
                char               *mapping;

                indexRel = relation_open(indexOids[i], AccessShareLock);
                desc     = alloc_index_descriptor(indexRel, false);

                mapping = TextDatumGetCString(make_es_mapping(heapRel->rd_id, heapRel->rd_att, false));
                desc->implementation->updateMapping(desc, mapping);

                relation_close(indexRel, AccessShareLock);
                pfree(mapping);
            }
        }
        relation_close(heapRel, AccessShareLock);
    }


    PG_RETURN_NULL();
}


Datum zdbcostestimate(PG_FUNCTION_ARGS) {
//	PlannerInfo  *root             = (PlannerInfo *) PG_GETARG_POINTER(0);
    IndexPath    *path             = (IndexPath *) PG_GETARG_POINTER(1);
//	double       loop_count        = PG_GETARG_FLOAT8(2);
    Cost         *indexStartupCost = (Cost *) PG_GETARG_POINTER(3);
    Cost         *indexTotalCost   = (Cost *) PG_GETARG_POINTER(4);
    Selectivity  *indexSelectivity = (Selectivity *) PG_GETARG_POINTER(5);
    double       *indexCorrelation = (double *) PG_GETARG_POINTER(6);
    IndexOptInfo *index            = path->indexinfo;
    ListCell     *lc;
    StringInfo   query             = makeStringInfo();
    int64        nhits             = 1;

    foreach (lc, path->indexclauses) {
        RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

        if (IsA(ri->clause, OpExpr)) {
            OpExpr *opExpr    = (OpExpr *) ri->clause;
            Node   *queryNode = (Node *) lsecond(opExpr->args);

            if (IsA(queryNode, Const)) {
                Const *queryConst = (Const *) queryNode;
                if (query->len > 0)
                    appendStringInfo(query, " AND ");
                if (list_length(path->indexclauses) > 1)
                    appendStringInfo(query, "(%s)", TextDatumGetCString(queryConst->constvalue));
                else
                    appendStringInfo(query, "%s", TextDatumGetCString(queryConst->constvalue));
            }
        }
    }

    if (query->len > 0) {
        ZDBIndexDescriptor *desc = zdb_alloc_index_descriptor_by_index_oid(index->indexoid);

        nhits = desc->implementation->estimateSelectivity(desc, query->data);
    }

    *indexStartupCost = (Cost) 0;
    *indexTotalCost   = (Cost) 0;
    *indexSelectivity = (Selectivity) (nhits / index->tuples);
    *indexCorrelation = 0.0;

    *indexSelectivity = Min(*indexSelectivity, 1);
    *indexSelectivity = Max(*indexSelectivity, 0);

    freeStringInfo(query);
    PG_RETURN_VOID();
}

Datum
zdbtupledeletedtrigger(PG_FUNCTION_ARGS)
{
    TriggerData        *trigdata = (TriggerData *) fcinfo->context;
    TransactionId      currentTransactionId       = GetCurrentTransactionId();
    Oid                indexRelOid;
    ZDBIndexDescriptor *desc;
    Relation           xactRel;
    Datum              datum[2];
    static bool        isnull[2] = {false, false};

    /* make sure it's called as a trigger at all */
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "zdbtupledeletedtrigger: not called by trigger manager");

    if (!(TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) ||
          TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)))
        elog(ERROR, "zdbtupledeletedtrigger: can only be fired for UPDATE and DELETE");

    if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
        elog(ERROR, "zdbtupledeletedtrigger: can only be fired as a BEFORE trigger");

    if (trigdata->tg_trigger->tgnargs != 1)
        elog(ERROR, "zdbtupledeletedtrigger: must specify the index oid as the only argument");

    indexRelOid = (Oid) atoi(trigdata->tg_trigger->tgargs[0]);
    desc        = zdb_alloc_index_descriptor_by_index_oid(indexRelOid);
    datum[0]    = Int64GetDatum(convert_xid(currentTransactionId));
    datum[1]    = BoolGetDatum(TRIGGER_FIRED_BY_INSERT(trigdata->tg_event));

    /*
     * add a row into our xact index to record the item pointer of the tuple being updated
     * this is its existing tid, not the new one
     */
    xactRel = relation_open(desc->xactRelId, AccessShareLock);
    index_insert(xactRel, datum, isnull, &trigdata->tg_trigtuple->t_self, trigdata->tg_relation, UNIQUE_CHECK_NO);
    relation_close(xactRel, AccessShareLock);

    /* remember this transaction id so we don't add an 'is_insert' pointer into
     * the xact index for any of the new tuples
     */
    lastxid = currentTransactionId;

    /* return the right thing */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        return PointerGetDatum(trigdata->tg_newtuple);
    else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
        return PointerGetDatum(trigdata->tg_trigtuple);
    else
        elog(ERROR, "trigger called in wrong context");
}