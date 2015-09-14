/*
 * Copyright 2013-2015 Technology Concepts & Design, Inc
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

#include "miscadmin.h"
#include "access/heapam_xlog.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "commands/event_trigger.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/builtins.h"

#include "zdb_interface.h"
#include "zdbops.h"
#include "zdbam.h"


PG_FUNCTION_INFO_V1(zdbbuild);
PG_FUNCTION_INFO_V1(zdbbuildempty);
PG_FUNCTION_INFO_V1(zdbinsert);
PG_FUNCTION_INFO_V1(zdbbeginscan);
PG_FUNCTION_INFO_V1(zdbgettuple);
PG_FUNCTION_INFO_V1(zdbgetbitmap);
PG_FUNCTION_INFO_V1(zdbrescan);
PG_FUNCTION_INFO_V1(zdbendscan);
PG_FUNCTION_INFO_V1(zdbmarkpos);
PG_FUNCTION_INFO_V1(zdbrestpos);
PG_FUNCTION_INFO_V1(zdbbulkdelete);
PG_FUNCTION_INFO_V1(zdbvacuumcleanup);
PG_FUNCTION_INFO_V1(zdboptions);
PG_FUNCTION_INFO_V1(zdbcostestimate);
PG_FUNCTION_INFO_V1(zdbsel);
PG_FUNCTION_INFO_V1(zdbtupledeletedtrigger);
PG_FUNCTION_INFO_V1(zdbeventtrigger);

PG_FUNCTION_INFO_V1(zdb_num_hits);


/* Working state for zdbbuild and its callback */
typedef struct
{
	bool     isUnique;
	bool     haveDead;
	Relation heapRel;

	double indtuples;

	ZDBIndexDescriptor *desc;
} ZDBBuildState;

typedef struct
{
	ZDBIndexDescriptor *indexDescriptor;
	uint64             nhits;
	int                currhit;
	ZDBSearchResponse  *hits;
	char               **queries;
	int                nqueries;
} ZDBScanState;

static void zdbbuildCallback(Relation indexRel, HeapTuple htup, Datum *values, bool *isnull, bool tupleIsAlive, void *state);

static bool needBatchFinishOnCommit = false;
static bool needBatchFinishOnCommit_set = false;
static bool didBatchFinishOnExecutorEnd = false;

static List *usedIndexesList     = NULL;
static List *indexesInsertedList = NULL;
static List *xactCommitDataList  = NULL;

static ExecutorStart_hook_type prev_ExecutorStartHook = NULL;
static ExecutorEnd_hook_type   prev_ExecutorEndHook  = NULL;
static int executorDepth = 0;
static int64 numHitsFound = -1;

extern HTAB *scan;

static Oid *findZDBIndexes(Oid relid, int *many)
{
	Oid *indexes = NULL;
	StringInfo sql;
	int i;

	SPI_connect();

	sql = makeStringInfo();
	appendStringInfo(sql, "select indexrelid "
			"from pg_index "
			"where indrelid = %d "
			"  and indclass[0] = (select oid from pg_opclass where opcmethod = (select oid from pg_am where amname = 'zombodb'))", relid);

	SPI_execute(sql->data, true, 1);
	*many = SPI_processed;
	if (SPI_processed > 0)
	{
		indexes = (Oid *) MemoryContextAlloc(TopTransactionContext, sizeof(Oid) * SPI_processed);
		for (i=0; i<SPI_processed; i++)
			indexes[i] =(Oid) atoi(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
	}
	SPI_finish();

	return indexes;
}

static ZDBIndexDescriptor *alloc_index_descriptor(Relation indexRel, bool forInsert)
{
	MemoryContext      oldContext = MemoryContextSwitchTo(TopTransactionContext);
	ListCell           *lc;
	ZDBIndexDescriptor *desc      = NULL;

	foreach (lc, usedIndexesList)
	{
		ZDBIndexDescriptor *tmp = lfirst(lc);

		if (tmp->indexRelid == RelationGetRelid(indexRel))
		{
			desc = tmp;
			break;
		}
	}

	if (desc == NULL)
	{
		desc            = zdb_alloc_index_descriptor(indexRel);
		usedIndexesList = lappend(usedIndexesList, desc);
	}

	if (forInsert && !desc->isShadow)
		indexesInsertedList = list_append_unique(indexesInsertedList, desc);

	MemoryContextSwitchTo(oldContext);
	return desc;
}

static void xact_complete_cleanup(XactEvent event) {
    ListCell *lc;

    List *usedIndexes = usedIndexesList;

    /* free up our static vars on xact finish */
    usedIndexesList     = NULL;
    indexesInsertedList = NULL;
    xactCommitDataList  = NULL;
	needBatchFinishOnCommit = false;
	needBatchFinishOnCommit_set = false;
	executorDepth = 0;
	numHitsFound = -1;
	scan = NULL;

	zdb_transaction_finish();

    /*
     * release any advisory locks we might still be holding
     *
     * We could definitely have some if the transaction aborted
     */
    DirectFunctionCall1(pg_advisory_unlock_all, PointerGetDatum(NULL));

    /* notify each index we used that the xact is over */
    foreach(lc, usedIndexes)
    {
        ZDBIndexDescriptor *desc = lfirst(lc);

        desc->implementation->transactionFinish(desc, event == XACT_EVENT_COMMIT ? ZDB_TRANSACTION_COMMITTED : ZDB_TRANSACTION_ABORTED);
    }
}

static void zdbam_commit_xact_data()
{
	if (xactCommitDataList)
	{
		ListCell *lc, *lc2;

		foreach(lc, usedIndexesList)
		{
			ZDBIndexDescriptor *outerDesc = lfirst(lc);
			List               *batch     = NULL;

			if (outerDesc->isShadow)
				continue;

			foreach(lc2, xactCommitDataList)
			{
				ZDBCommitXactData *data = lfirst(lc2);

				if (zdb_index_descriptors_equal(outerDesc, data->desc))
				{
					batch = lappend(batch, data);
				}
			}

			if (batch)
			{
				outerDesc->implementation->commitXactData(outerDesc, batch);
			}
			list_free(batch);
		}
	}

}

static void zdbam_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:

			if (needBatchFinishOnCommit || !didBatchFinishOnExecutorEnd)
			{
				ListCell *lc;

				/* finish the batch insert (and refresh) for each index into which new records were inserted */
				foreach (lc, indexesInsertedList)
				{
					ZDBIndexDescriptor *desc = lfirst(lc);
					desc->implementation->batchInsertFinish(desc);
				}
			}


			/*
			 * if the transaction is about to commit we need to push the list of modified ctids
			 */
			zdbam_commit_xact_data();
            break;

		case XACT_EVENT_ABORT:
            break;

		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PREPARE:
			elog(ERROR, "zombodb doesn't support prepared transactions");
			break;

		default:
			break;
	}

    /*
     * If the transaction is finished, make sure to get it cleaned up
     */

    switch (event)
    {
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_ABORT:
            /* cleanup, the xact is over */
            xact_complete_cleanup(event);
            break;

        default:
            break;
    }


}

static void zdb_executor_start_hook(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStartHook == zdb_executor_start_hook)
		elog(ERROR, "zdb_executor_start_hook: Somehow prev_ExecutorStartHook was set to zdb_executor_start_hook");

	executorDepth++;

	if (prev_ExecutorStartHook)
		prev_ExecutorStartHook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

static void zdb_executor_end_hook(QueryDesc *queryDesc)
{
	executorDepth--;

	if (executorDepth == 0)
	{
		if (!needBatchFinishOnCommit_set)
		{
			/*
			 * If the connected application_name is "postgres_fdw" then we're going to defer all
			 * batchInsertFinish calls until the transaction commits
			 *
			 * The the application_name changes while the transaction is running, well, we won't
			 * detect that.
			 */
			needBatchFinishOnCommit     = strcmp(application_name, "postgres_fdw") == 0 || strcmp(application_name, "dwf") == 0;
			needBatchFinishOnCommit_set = true;
		}

		if (!needBatchFinishOnCommit)
		{
			ListCell *lc;

			/* finish the batch insert (and refresh) for each index into which new records were inserted */
			foreach (lc, indexesInsertedList)
			{
				ZDBIndexDescriptor *desc = lfirst(lc);
				desc->implementation->batchInsertFinish(desc);
			}
			didBatchFinishOnExecutorEnd = true;
		}
	}

	if (prev_ExecutorEndHook == zdb_executor_end_hook)
		elog(ERROR, "zdb_executor_end_hook: Somehow prev_ExecutorEndHook was set to zdb_executor_end_hook");

	if (prev_ExecutorEndHook)
		prev_ExecutorEndHook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

void zdbam_init(void)
{
	zdb_index_init();

	if (prev_ExecutorStartHook == zdb_executor_start_hook)
		elog(ERROR, "zdbam_init:  Unable to initialize ZomboDB.  ExecutorStartHook already assigned");
	else if (prev_ExecutorEndHook == zdb_executor_end_hook)
		elog(ERROR, "zdbam_init:  Unable to initialize ZomboDB.  ExecutorEndHook already assigned");

	prev_ExecutorStartHook = ExecutorStart_hook;
	prev_ExecutorEndHook   = ExecutorEnd_hook;

	ExecutorStart_hook   = zdb_executor_start_hook;
	ExecutorEnd_hook     = zdb_executor_end_hook;

	RegisterXactCallback(zdbam_xact_callback, NULL);
}

void zdbam_fini(void)
{
	if (prev_ExecutorEndHook == zdb_executor_end_hook)
		elog(ERROR, "zdbam_fini: Somehow prev_ExecutorEndHook was set to zdb_executor_end_hook");

	ExecutorStart_hook = prev_ExecutorStartHook;
	ExecutorEnd_hook   = prev_ExecutorEndHook;

	UnregisterXactCallback(zdbam_xact_callback, NULL);
}

/*
 *  zdbbuild() -- build a new zombodb index.
 */
Datum
zdbbuild(PG_FUNCTION_ARGS)
{
	Relation         heapRel    = (Relation) PG_GETARG_POINTER(0);
	Relation         indexRel   = (Relation) PG_GETARG_POINTER(1);
	IndexInfo        *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
	IndexBuildResult *result;
	double           reltuples = 0.0;
	ZDBBuildState    buildstate;
	StringInfo       triggerSQL;
	Datum PROPERTIES = CStringGetTextDatum("properties");
	Datum mappingDatum;
	Datum propertiesDatum;
	char *properties;

	if (heapRel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
		elog(ERROR, "ZomoboDB indexes not supported on unlogged tables");

	if (!ZDBIndexOptionsGetUrl(indexRel) && !ZDBIndexOptionsGetShadow(indexRel))
		elog(ERROR, "Must set the 'url' or 'shadow' index option");

	buildstate.isUnique    = indexInfo->ii_Unique;
	buildstate.haveDead    = false;
	buildstate.heapRel     = heapRel;
	buildstate.indtuples   = 0;
	buildstate.desc        = alloc_index_descriptor(indexRel, false);
	buildstate.desc->logit = true;

	if (!buildstate.desc->isShadow)
	{
		/* drop the existing index */
		buildstate.desc->implementation->dropIndex(buildstate.desc);

		/* create a new, empty index */
		mappingDatum    = make_es_mapping(heapRel->rd_att, false);
		propertiesDatum = DirectFunctionCall2(json_object_field, mappingDatum, PROPERTIES);
		properties      = TextDatumGetCString(propertiesDatum);

		buildstate.desc->implementation->createNewIndex(buildstate.desc, ZDBIndexOptionsGetNumberOfShards(indexRel), ZDBIndexOptionsGetNoXact(indexRel), properties);

		/* do the heap scan */
		reltuples = IndexBuildHeapScan(heapRel, indexRel, indexInfo, false, zdbbuildCallback, (void *) &buildstate);

		/* signal that the batch inserts have stopped */
		buildstate.desc->implementation->batchInsertFinish(buildstate.desc);

		/* force the index to refresh so the rows are immediately available */
		buildstate.desc->implementation->refreshIndex(buildstate.desc);

		/* reset the settings to reasonable values for production use */
		buildstate.desc->implementation->finalizeNewIndex(buildstate.desc);

		if (heapRel->rd_rel->relkind != 'm')
		{
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
								"SELECT oid "
								"       FROM pg_trigger "
								"       WHERE tgname = 'zzzzdb_tuple_sync_for_%d_using_%d'",
						RelationGetRelid(heapRel), RelationGetRelid(indexRel), buildstate.desc->schemaName, buildstate.desc->tableName, RelationGetRelid(indexRel),  /* CREATE TRIGGER args */
						RelationGetRelid(heapRel), RelationGetRelid(indexRel) /* SELECT FROM pg_trigger args */
				);

				if (SPI_execute(triggerSQL->data, false, 0) == SPI_OK_SELECT && SPI_processed == 1)
				{
					ObjectAddress indexAddress;
					ObjectAddress triggerAddress;

					indexAddress.classId     = RelationRelationId;
					indexAddress.objectId    = RelationGetRelid(indexRel);
					indexAddress.objectSubId = 0;

					triggerAddress.classId     = TriggerRelationId;
					triggerAddress.objectId    = (Oid) atoi(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
					triggerAddress.objectSubId = 0;

					recordDependencyOn(&triggerAddress, &indexAddress, DEPENDENCY_INTERNAL);
				}
				else
				{
					elog(ERROR, "Cannot create trigger");
				}
			}

			pfree(triggerSQL->data);
			pfree(triggerSQL);

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
static void
zdbbuildCallback(Relation indexRel,
		HeapTuple htup,
		Datum *values,
		bool *isnull,
		bool tupleIsAlive,
		void *state)
{
	ZDBBuildState      *buildstate = (ZDBBuildState *) state;
	ZDBIndexDescriptor *desc       = buildstate->desc;
	text               *value      = DatumGetTextP(values[1]);

	if (HeapTupleIsHeapOnly(htup))
		elog(ERROR, "Heap Only Tuple (HOT) found at (%d, %d).  Run VACUUM FULL %s; and reindex", ItemPointerGetBlockNumber(&(htup->t_self)), ItemPointerGetOffsetNumber(&(htup->t_self)), desc->qualifiedTableName);

	desc->implementation->batchInsertRow(
			desc,
			&htup->t_self,
			HeapTupleHeaderGetXmin(htup->t_data),
			HeapTupleHeaderGetRawXmax(htup->t_data),
			HeapTupleHeaderGetRawCommandId(htup->t_data),
			HeapTupleHeaderGetRawCommandId(htup->t_data),
			(htup->t_data->t_infomask & HEAP_XMIN_COMMITTED) != 0,
			(htup->t_data->t_infomask & HEAP_XMAX_COMMITTED) != 0,
			value
	);

	buildstate->indtuples += 1;
}

/*
 *  zdbinsert() -- insert an index tuple into a zombodb.
 */
Datum
zdbinsert(PG_FUNCTION_ARGS)
{
	MemoryContext      oldContext;
	Relation           indexRel = (Relation) PG_GETARG_POINTER(0);
	Datum              *values  = (Datum *) PG_GETARG_POINTER(1);
//    bool	   *isnull = (bool *) PG_GETARG_POINTER(2);
	ItemPointer        ht_ctid  = (ItemPointer) PG_GETARG_POINTER(3);
//    Relation	heapRel = (Relation) PG_GETARG_POINTER(4);
//    IndexUniqueCheck checkUnique = (IndexUniqueCheck) PG_GETARG_INT32(5);
	ZDBIndexDescriptor *desc;
	text               *value   = DatumGetTextP(values[1]);

	desc = alloc_index_descriptor(indexRel, true);
	if (desc->isShadow)
		PG_RETURN_BOOL(false);

	oldContext = MemoryContextSwitchTo(TopTransactionContext);
	desc->implementation->batchInsertRow(desc, ht_ctid, GetCurrentTransactionId(), 0, GetCurrentCommandId(false), GetCurrentCommandId(false), false, false, value);
	xactCommitDataList = lappend(xactCommitDataList, zdb_alloc_new_xact_record(desc, ht_ctid));
	MemoryContextSwitchTo(oldContext);

	PG_RETURN_BOOL(true);
}

/*
 *	zdbbuildempty() -- build an empty zombodb index in the initialization fork
 */
Datum
zdbbuildempty(PG_FUNCTION_ARGS)
{
//    Relation	index = (Relation) PG_GETARG_POINTER(0);

	PG_RETURN_VOID();
}

Datum
zdb_num_hits(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(numHitsFound);
}

static void setup_scan(IndexScanDesc scan)
{
	ZDBScanState       *scanstate = (ZDBScanState *) scan->opaque;
	ZDBIndexDescriptor *desc      = alloc_index_descriptor(scan->indexRelation, false);
	char               **queries;
	int                i;

	if (scanstate->hits)
		scanstate->indexDescriptor->implementation->freeSearchResponse(scanstate->hits);

	if (scanstate->queries)
	{
		for (i = 0; i < scanstate->nqueries; i++)
			pfree(scanstate->queries[i]);
		pfree(scanstate->queries);
	}

	queries = palloc0(scan->numberOfKeys * sizeof(char *));
	for (i = 0; i < scan->numberOfKeys; i++)
	{
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
	scanstate->hits            = desc->implementation->searchIndex(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), queries, scan->numberOfKeys, &scanstate->nhits);
	scanstate->currhit         = 0;

	numHitsFound = scanstate->hits->total_hits;
}

__inline static void set_item_pointer(ZDBSearchResponse *data, int index, ItemPointer target)
{
	BlockNumber  blkno;
	OffsetNumber offno;

	memcpy(&blkno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber))), sizeof(BlockNumber));
	memcpy(&offno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber)) + sizeof(BlockNumber)), sizeof(OffsetNumber));

	ItemPointerSet(target, blkno, offno);
}

/*
 *  zdbgettuple() -- Get the next tuple in the scan.
 */
Datum
zdbgettuple(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan       = (IndexScanDesc) PG_GETARG_POINTER(0);
	ZDBScanState  *scanstate = (ZDBScanState *) scan->opaque;
//    ScanDirection dir = (ScanDirection) PG_GETARG_INT32(1);
	bool          haveMore;

	/* zdbtree indexes are never lossy */
	scan->xs_recheck = false;

	haveMore = scanstate->currhit < scanstate->nhits;
	if (haveMore)
	{
		set_item_pointer(scanstate->hits, scanstate->currhit, &scan->xs_ctup.t_self);
		scanstate->currhit++;
	}

	PG_RETURN_BOOL(haveMore);
}

/*
 *  zdbgetbitmap() -- gets all matching tuples, and adds them to a bitmap
 */
Datum
zdbgetbitmap(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan       = (IndexScanDesc) PG_GETARG_POINTER(0);
	TIDBitmap     *tbm       = (TIDBitmap *) PG_GETARG_POINTER(1);
	ZDBScanState  *scanstate = (ZDBScanState *) scan->opaque;
	int           i;

	for (i = 0; i < scanstate->nhits; i++)
	{
		ItemPointerData target;

		CHECK_FOR_INTERRUPTS();

		set_item_pointer(scanstate->hits, i, &target);

		tbm_add_tuples(tbm, &target, 1, false);
	}

	PG_RETURN_INT64(scanstate->nhits);
}

/*
 *  zdbbeginscan() -- start a scan on a zombodb index
 */
Datum
zdbbeginscan(PG_FUNCTION_ARGS)
{
	Relation      rel       = (Relation) PG_GETARG_POINTER(0);
	int           nkeys     = PG_GETARG_INT32(1);
	int           norderbys = PG_GETARG_INT32(2);
	IndexScanDesc scan;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	scan->opaque       = (ZDBScanState *) palloc0(sizeof(ZDBScanState));

	PG_RETURN_POINTER(scan);
}

/*
 *  zdbrescan() -- rescan an index relation
 */
Datum
zdbrescan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan       = (IndexScanDesc) PG_GETARG_POINTER(0);
	ScanKey       scankey    = (ScanKey) PG_GETARG_POINTER(1);
	ZDBScanState  *scanstate = (ZDBScanState *) scan->opaque;
	bool          changed    = false;

	if (scankey && scan->numberOfKeys > 0)
	{
		if (scanstate->queries)
		{
			/*
			 * we've already called setup_scan() so
			 * lets see if our scankeys have changed
			 */

			if (scan->numberOfKeys == scanstate->nqueries)
			{
				int i;
				for (i = 0; i < scan->numberOfKeys; i++)
				{
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
			}
			else
			{
				changed = true;
			}
		}
	}

	if (changed || scanstate->queries == NULL)
	{
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));

		setup_scan(scan);
	}
	else
	{
		scanstate->currhit = 0;
	}

	PG_RETURN_VOID();
}

/*
 *  zdbendscan() -- close down a scan
 */
Datum
zdbendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan       = (IndexScanDesc) PG_GETARG_POINTER(0);
	ZDBScanState  *scanstate = (ZDBScanState *) scan->opaque;

	if (scanstate)
	{
		if (scanstate->hits)
		{
			scanstate->indexDescriptor->implementation->freeSearchResponse(scanstate->hits);
			scanstate->hits = NULL;
		}

		if (scanstate->queries)
		{
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
Datum
zdbmarkpos(PG_FUNCTION_ARGS)
{
//    IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);

	// TODO:  remember where we are in the current scan
	elog(NOTICE, "zdbmarkpos()");

	PG_RETURN_VOID();
}

/*
 *  zdbrestrpos() -- restore scan to last saved position
 */
Datum
zdbrestrpos(PG_FUNCTION_ARGS)
{
//    IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);

	// TODO:  take us back to where we last markpos()'d
	elog(NOTICE, "zdbrestrpos()");

	PG_RETURN_VOID();
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum
zdbbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback        = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void                    *callback_state = (void *) PG_GETARG_POINTER(3);
	Relation                indexRel        = info->index;
	ZDBIndexDescriptor      *desc;
	ZDBSearchResponse       *items;
	List                    *to_delete      = NULL;
	int                     many            = 0;
	uint64                  nitems;
	int                     i;

	/* allocate stats if first time through, else re-use existing struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	desc = alloc_index_descriptor(indexRel, false);
	if (desc->isShadow)
		PG_RETURN_POINTER(stats);

	items = desc->implementation->getPossiblyExpiredItems(desc, &nitems);

	for (i = 0; i < nitems; i++)
	{
		ItemPointerData *ctid = palloc(sizeof(ItemPointerData));

		CHECK_FOR_INTERRUPTS();

		set_item_pointer(items, i, ctid);
		if (!ItemPointerIsValid(ctid))
			continue;

		if (callback(ctid, callback_state))
		{
			to_delete = lappend(to_delete, ctid);
			many++;
		}
	}

	if (many > 0)
	{
		desc->implementation->bulkDelete(desc, to_delete, many);
	}

	desc->implementation->freeSearchResponse(items);

	PG_RETURN_POINTER(stats);
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum
zdbvacuumcleanup(PG_FUNCTION_ARGS)
{
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
	if (!info->estimated_count)
	{
		if (stats->num_index_tuples > info->num_heap_tuples)
			stats->num_index_tuples = info->num_heap_tuples;
	}

	PG_RETURN_POINTER(stats);
}

Datum
zdboptions(PG_FUNCTION_ARGS)
{
	Datum                         reloptions = PG_GETARG_DATUM(0);
	bool                          validate   = PG_GETARG_BOOL(1);
	relopt_value                  *options;
	ZDBIndexOptions               *rdopts;
	int                           numoptions;
	static const relopt_parse_elt tab[]      = {
			{"url", RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, urlValueOffset)},
			{"shadow", RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, shadowValueOffset)},
			{"options", RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, optionsValueOffset)},
			{"preference", RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, preferenceValueOffset)},
			{"shards", RELOPT_TYPE_INT, offsetof(ZDBIndexOptions, shards)},
			{"replicas", RELOPT_TYPE_INT, offsetof(ZDBIndexOptions, replicas)},
			{"noxact", RELOPT_TYPE_BOOL, offsetof(ZDBIndexOptions, noxact)},
			{"bulk_concurrency", RELOPT_TYPE_INT, offsetof(ZDBIndexOptions, bulk_concurrency)},
			{"batch_size", RELOPT_TYPE_INT, offsetof(ZDBIndexOptions, batch_size)}
	};

	options = parseRelOptions(reloptions, validate, RELOPT_KIND_ZDB,
			&numoptions);

	/* if none set, we're done */
	if (numoptions == 0)
		PG_RETURN_NULL();

	rdopts = allocateReloptStruct(sizeof(ZDBIndexOptions), options, numoptions);

	fillRelOptions((void *) rdopts, sizeof(ZDBIndexOptions), options, numoptions,
			validate, tab, lengthof(tab));

	pfree(options);

	PG_RETURN_BYTEA_P(rdopts);
}

Datum
zdbcostestimate(PG_FUNCTION_ARGS)
{
//    PlannerInfo *root = (PlannerInfo * )PG_GETARG_POINTER(0);
//    IndexPath *path = (IndexPath * )PG_GETARG_POINTER(1);
//    double loop_count = PG_GETARG_FLOAT8(2);
	Cost        *indexStartupCost = (Cost *) PG_GETARG_POINTER(3);
	Cost        *indexTotalCost   = (Cost *) PG_GETARG_POINTER(4);
	Selectivity *indexSelectivity = (Selectivity *) PG_GETARG_POINTER(5);
	double      *indexCorrelation = (double *) PG_GETARG_POINTER(6);
//    IndexOptInfo *index = path->indexinfo;

	*indexStartupCost = 0;
	*indexTotalCost   = 0.0001;
	*indexSelectivity = 0.0001;
	*indexCorrelation = 0.0001;

	PG_RETURN_VOID();
}

Datum
zdbsel(PG_FUNCTION_ARGS)
{
	// TODO:  not sure exactly what we should do here
	// TODO:  figure out which table (and then index)
	// TODO:  and run the query or just continue to
	// TODO:  return a really small number?
//	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
//	Oid			operator = PG_GETARG_OID(1);
//	List	   *args = (List *) PG_GETARG_POINTER(2);
//	int			varRelid = PG_GETARG_INT32(3);
//	FuncExpr *left = (FuncExpr *) linitial(args);
//
//	Var *firstArg = (Var *) linitial(left->args);
//
//	elog(NOTICE, "zdbsel: valRelid=%d, tag=%d", varRelid, firstArg->vartype);
	PG_RETURN_FLOAT8((float8) 0.0001);
}

Datum
zdbtupledeletedtrigger(PG_FUNCTION_ARGS)
{
	MemoryContext      oldContext;
	TriggerData        *trigdata = (TriggerData *) fcinfo->context;
	ZDBIndexDescriptor *desc;
	Oid                indexRelOid;
	Relation           indexRel;

	/* make sure it's called as a trigger at all */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "zdbtupledeletedtrigger: not called by trigger manager");

	if (!(TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) || TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)))
		elog(ERROR, "zdbtupledeletedtrigger: can only be fired for UPDATE and DELETE");

	if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
		elog(ERROR, "zdbtupledeletedtrigger: can only be fired as a BEFORE trigger");

	if (trigdata->tg_trigger->tgnargs != 1)
		elog(ERROR, "zdbtupledeletedtrigger: must specify the index oid as the only argument");

	indexRelOid = (Oid) atoi(trigdata->tg_trigger->tgargs[0]);

	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	indexRel = relation_open(indexRelOid, AccessShareLock);
	desc     = alloc_index_descriptor(indexRel, false);
	relation_close(indexRel, AccessShareLock);

	/* record what is changing */
	xactCommitDataList = lappend(xactCommitDataList, zdb_alloc_expired_xact_record(desc, &trigdata->tg_trigtuple->t_self, GetCurrentTransactionId(), GetCurrentCommandId(false)));

	MemoryContextSwitchTo(oldContext);

	/* return the right thing */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		return PointerGetDatum(trigdata->tg_newtuple);
	else
		return PointerGetDatum(trigdata->tg_trigtuple);
}

Datum
zdbeventtrigger(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))  /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	trigdata = (EventTriggerData *) fcinfo->context;

	if (trigdata->parsetree->type == T_AlterTableStmt)
	{
		AlterTableStmt *stmt = (AlterTableStmt *) trigdata->parsetree;
		Relation heapRel = heap_openrv(stmt->relation, AccessShareLock);
		Oid *indexOids;
		int many;

		indexOids = findZDBIndexes(RelationGetRelid(heapRel), &many);
		if (many > 0 && indexOids != NULL)
		{
			int i;
			for (i=0; i<many; i++)
			{
				Relation indexRel;
				ZDBIndexDescriptor *desc;
				char *mapping;

				indexRel = relation_open(indexOids[i], AccessShareLock);
				desc = alloc_index_descriptor(indexRel, false);

				mapping = TextDatumGetCString(make_es_mapping(heapRel->rd_att, false));
				desc->implementation->updateMapping(desc, mapping);

				relation_close(indexRel, AccessShareLock);
				pfree(mapping);
			}
		}
		relation_close(heapRel, AccessShareLock);
	}


	PG_RETURN_NULL();
}
