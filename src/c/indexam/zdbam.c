/**
 * Copyright 2018 ZomboDB, LLC
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

#include "zdbam.h"

#include "elasticsearch/querygen.h"
#include "highlighting/highlighting.h"
#include "scoring/scoring.h"

#include "access/amapi.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_trigger.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "optimizer/cost.h"
#include "parser/parse_func.h"
#include "tcop/utility.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/lsyscache.h"

static const struct config_enum_entry zdb_log_level_options[] = {
		{"debug",   DEBUG2,  true},
		{"debug5",  DEBUG5,  false},
		{"debug4",  DEBUG4,  false},
		{"debug3",  DEBUG3,  false},
		{"debug2",  DEBUG2,  false},
		{"debug1",  DEBUG1,  false},
		{"info",    INFO,    false},
		{"notice",  NOTICE,  false},
		{"warning", WARNING, false},
		{"log",     LOG,     false},
		{NULL, 0,            false}
};

typedef struct ZDBBuildStateData {
	double                   indtuples;
	ElasticsearchBulkContext *esContext;
	MemoryContext            memoryContext;
}                                     ZDBBuildStateData;

typedef struct ZDBScanContext {
	bool                       needsInit;
	ElasticsearchScrollContext *scrollContext;
	float4                     lastScore;
	ItemPointerData            lastCtid;
	HTAB                       *scoreLookup;
	HTAB                       *highlightLookup;
	bool                       wantScores;
	bool                       wantHighlights;
	ZDBQueryType               *query;
}                                     ZDBScanContext;

PG_FUNCTION_INFO_V1(zdb_delete_trigger);
PG_FUNCTION_INFO_V1(zdb_update_trigger);

void zdb_aminit(void);
bool zdbamvalidate(Oid opclassoid);
static IndexBuildResult *ambuild(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo);
static void ambuildempty(Relation indexRelation);
static bool aminsert(Relation indexRelation, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRelation, IndexUniqueCheck checkUnique, IndexInfo *indexInfo);
static IndexBulkDeleteResult *ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
static IndexBulkDeleteResult *amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
static IndexBulkDeleteResult *zdb_vacuum_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, bool via_cleanup);
static void amcostestimate(struct PlannerInfo *root, struct IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation, double *indexPages);
static bytea *amoptions(Datum reloptions, bool validate);
static IndexScanDesc ambeginscan(Relation indexRelation, int nkeys, int norderbys);
static void amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
static bool amgettuple(IndexScanDesc scan, ScanDirection direction);
static void amendscan(IndexScanDesc scan);
static int64 amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);

static void zdbbuildCallback(Relation indexRel, HeapTuple htup, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void index_record(ElasticsearchBulkContext *esContext, MemoryContext scratchContext, ItemPointer ctid, Datum record, HeapTuple htup);

static void apply_alter_statement(PlannedStmt *parsetree, char *url, uint32 shards, char *typeName, char *oldAlias, char *oldUUID);
static Relation open_relation_from_parsetree(PlannedStmt *parsetree, LOCKMODE lockmode, bool *is_index);
static void get_immutable_index_options(PlannedStmt *parsetree, char **url, uint32 *shards, char **typeName, char **alias, char **uuid);


/*lint -esym 715,extra,source ignore unused param */
static bool validate_default_elasticsearch_url(char **newval, void **extra, GucSource source) {
	/* valid only if it's NULL or ends with a forward slash */
	char *str = *newval;
	return str == NULL || str[strlen(str) - 1] == '/';
}

static void validate_url(char *str) {
	/* valid only if it ends with a forward slash or it equals the string 'default' */
	if (str != NULL && (str[strlen(str) - 1] == '/' || strcmp("default", str) == 0))
		return;

	elog(ERROR, "'url' index option must end in a slash");
}

/*lint -esym 715,str ignore unused param */
static void validate_type_name(char *str) {
	/* noop */
}

/*lint -esym 715,str ignore unused param */
static void validate_refresh_interval(char *str) {
	/* noop */
}

/*lint -esym 715,str ignore unused param */
static void validate_alias(char *str) {
	/* noop */
}

/*lint -esym 715,str ignore unused param */
static void validate_uuid(char *str) {
	/* noop */
}


PG_FUNCTION_INFO_V1(zdb_amhandler);

static List                     *insert_contexts        = NULL;
static List                     *to_drop                = NULL;
static List                     *aborted_xids           = NULL;
static ExecutorStart_hook_type  prev_ExecutorStartHook  = NULL;
static ExecutorEnd_hook_type    prev_ExecutorEndHook    = NULL;
static ExecutorRun_hook_type    prev_ExecutorRunHook    = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinishHook = NULL;
static ProcessUtility_hook_type prev_ProcessUtilityHook = NULL;
static int                      executor_depth          = 0;

int  ZDB_LOG_LEVEL;
char *zdb_default_elasticsearch_url_guc;
int  zdb_default_row_estimation_guc;
bool zdb_curl_verbose_guc;
bool zdb_ignore_visibility_guc;
int  zdb_default_replicas_guc;

relopt_kind RELOPT_KIND_ZDB;

List *currentQueryStack = NULL;

void finish_inserts(bool is_commit) {
	ListCell *lc;

	foreach (lc, insert_contexts) {
		ZDBIndexChangeContext *context = lfirst(lc);
		ListCell *lc2;

		foreach(lc2, aborted_xids) {
			(void) list_delete_int(context->esContext->usedXids, lfirst_int(lc2));
		}

		ElasticsearchFinishBulkProcess(context->esContext, is_commit);
	}

}

ArrayType *collect_used_xids(MemoryContext memoryContext) {
	ListCell *lc;
	ArrayBuildState *astate = NULL;

	foreach (lc, insert_contexts) {
		ZDBIndexChangeContext *context = lfirst(lc);
		ListCell *lc2;

		foreach (lc2, context->esContext->usedXids) {
			uint64 xid = convert_xid((TransactionId) lfirst_int(lc2));

			if (!list_member_int(aborted_xids, lfirst_int(lc2))) {
				astate = accumArrayResult(astate, UInt64GetDatum(xid), false, INT8OID, memoryContext);
			}
		}
	}

	astate = accumArrayResult(astate, UInt64GetDatum(GetCurrentTransactionIdIfAny()), false, INT8OID, memoryContext);

	return DatumGetArrayTypeP(makeArrayResult(astate, memoryContext));
}

/*lint -esym 715,mySubid,parentSubid,arg ignore unused param */
static void subxact_callback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg) {
	switch (event) {
		case SUBXACT_EVENT_ABORT_SUB: {
			TransactionId curr_xid = GetCurrentTransactionIdIfAny();

			if (curr_xid != InvalidTransactionId) {
				MemoryContext oldContext;

				oldContext = MemoryContextSwitchTo(TopTransactionContext);
				aborted_xids = lappend_int(aborted_xids, (int) curr_xid);
				MemoryContextSwitchTo(oldContext);
			}
		} break;

		default:
			break;
	}
}


/*lint -esym 715,arg ignore unused param */
static void xact_commit_callback(XactEvent event, void *arg) {
	/* when about to commit, finish any inserts we might not yet have sent to Elasticsearch */
	switch (event) {
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE: {
			ListCell *lc;

			HOLD_INTERRUPTS();

			finish_inserts(true);

			foreach(lc, to_drop) {
				char *index_url = lfirst(lc);
				ElasticsearchDeleteIndexDirect(index_url);
			}

			RESUME_INTERRUPTS();
		}
			break;
		default:
			break;
	}

	/* reset per-transaction state in any of the xact completion states */
	switch (event) {
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			executor_depth    = 0;
			insert_contexts   = NULL;
			to_drop           = NULL;
			aborted_xids      = NULL;
			currentQueryStack = NULL;
			break;
		default:
			break;
	}
}

static void push_executor_info(QueryDesc *queryDesc) {
	MemoryContext oldContext;

	/* push the current query onto our stack */
	oldContext        = MemoryContextSwitchTo(TopTransactionContext);
	currentQueryStack = lcons(queryDesc, currentQueryStack);
	MemoryContextSwitchTo(oldContext);

	executor_depth++;
}

static void pop_executor_info() {
	MemoryContext oldContext;

	/* pop the current query from our stack */
	oldContext        = MemoryContextSwitchTo(TopTransactionContext);
	currentQueryStack = list_delete_first(currentQueryStack);
	MemoryContextSwitchTo(oldContext);

	executor_depth--;
}

static void zdb_executor_start_hook(QueryDesc *queryDesc, int eflags) {
	push_executor_info(queryDesc);
	PG_TRY();
			{
				if (prev_ExecutorStartHook)
					prev_ExecutorStartHook(queryDesc, eflags);
				else
					standard_ExecutorStart(queryDesc, eflags);
				pop_executor_info();
			}
		PG_CATCH();
			{
				pop_executor_info();
				PG_RE_THROW();
			}
	PG_END_TRY();
}

static void zdb_executor_end_hook(QueryDesc *queryDesc) {
	/*
	 * If the statement has finished we need to do some cleanup work...
	 *
	 * We need to do these things before calling standard_ExecutorEnd() below because that
	 * will delete the MemoryContext our insert batches were allocated in, and we still
	 * need to use them here.
	 */
	if (executor_depth == 0) {

		/* cleanup any score and highlight tracking we might have */
		scoring_support_cleanup();
		highlight_support_cleanup();

		currentQueryStack = NULL;
	}

	push_executor_info(queryDesc);
	PG_TRY();
			{
				if (prev_ExecutorEndHook)
					prev_ExecutorEndHook(queryDesc);
				else
					standard_ExecutorEnd(queryDesc);
				pop_executor_info();
			}
		PG_CATCH();
			{
				pop_executor_info();
				PG_RE_THROW();
			}
	PG_END_TRY();
}

static void zdb_executor_run_hook(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
	push_executor_info(queryDesc);
	PG_TRY();
			{
				if (prev_ExecutorRunHook)
					prev_ExecutorRunHook(queryDesc, direction, count, execute_once);
				else
					standard_ExecutorRun(queryDesc, direction, count, execute_once);
				pop_executor_info();
			}
		PG_CATCH();
			{
				pop_executor_info();
				PG_RE_THROW();
			}
	PG_END_TRY();
}

static void zdb_executor_finish_hook(QueryDesc *queryDesc) {
	push_executor_info(queryDesc);
	PG_TRY();
			{
				if (prev_ExecutorFinishHook)
					prev_ExecutorFinishHook(queryDesc);
				else
					standard_ExecutorFinish(queryDesc);
				pop_executor_info();
			}
		PG_CATCH();
			{
				pop_executor_info();
				PG_RE_THROW();
			}
	PG_END_TRY();
}

static void run_process_utility_hook(PlannedStmt *parsetree, const char *queryString, ProcessUtilityContext context, ParamListInfo params, QueryEnvironment *queryEnv, DestReceiver *dest, char *completionTag) {
	/* ask Postgres to execute this utility statement */
	if (prev_ProcessUtilityHook)
		prev_ProcessUtilityHook(parsetree, queryString, context, params, queryEnv, dest, completionTag);
	else
		standard_ProcessUtility(parsetree, queryString, context, params, queryEnv, dest, completionTag);
}

static void zdb_process_utility_hook(PlannedStmt *parsetree, const char *queryString, ProcessUtilityContext context, ParamListInfo params, QueryEnvironment *queryEnv, DestReceiver *dest, char *completionTag) {
	executor_depth++;
	PG_TRY();
			{
				switch (nodeTag(parsetree->utilityStmt)) {
					case T_AlterTableStmt: {
						char   *url;
						uint32 shards;
						char   *typeName;
						char   *alias;
						char   *uuid;

						get_immutable_index_options(parsetree, &url, &shards, &typeName, &alias, &uuid);
						run_process_utility_hook(parsetree, queryString, context, params, queryEnv, dest,
												 completionTag);
						apply_alter_statement(parsetree, url, shards, typeName, alias, uuid);
					}
						break;

					case T_DropStmt: {
						MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
						DropStmt      *drop      = (DropStmt *) parsetree->utilityStmt;
						ListCell      *lc;

						foreach(lc, drop->objects) {
							switch (drop->removeType) {
								case OBJECT_TABLE:
								case OBJECT_MATVIEW:
								case OBJECT_INDEX:
								case OBJECT_SCHEMA: {
									ObjectAddress address;
									Node          *object = lfirst(lc);
									Relation      rel     = NULL;
									List          *names  = NULL, *args = NULL;
									char          *tmp;

									/* Get an ObjectAddress for the object. */
									address = get_object_address(drop->removeType, object, &rel, AccessExclusiveLock,
																 drop->missing_ok);

									if (address.objectId == InvalidOid) {
										/* the object doesn't exist, so there's nothing we need to do */
										break;
									}

									tmp = getObjectIdentityParts(&address, &names, &args);
									pfree(tmp);
									switch (drop->removeType) {
										case OBJECT_TABLE:
										case OBJECT_MATVIEW: {
											ListCell *lc2;

											foreach (lc2, RelationGetIndexList(rel)) {
												Relation indexRel = RelationIdGetRelation(lfirst_oid(lc2));
												if (index_is_zdb_index(indexRel) &&
													!ZDBIndexOptionsGetLLAPI(indexRel)) {
													to_drop = lappend(to_drop,
																	  psprintf("%s%s",
																			   ZDBIndexOptionsGetUrl(indexRel),
																			   ZDBIndexOptionsGetIndexName(indexRel)));
												}
												RelationClose(indexRel);
											}
										}
											break;

										case OBJECT_INDEX:
											if (index_is_zdb_index(rel) && !ZDBIndexOptionsGetLLAPI(rel)) {

												to_drop = lappend(to_drop,
																  psprintf("%s%s",
																		   ZDBIndexOptionsGetUrl(rel),
																		   ZDBIndexOptionsGetIndexName(rel)));
											}
											break;

										case OBJECT_SCHEMA: {
											List     *indexOids = lookup_zdb_indexes_in_namespace(address.objectId);
											ListCell *lc2;

											foreach (lc2, indexOids) {
												Oid      oid = lfirst_oid(lc2);
												Relation indexRel;

												indexRel = RelationIdGetRelation(oid);
												if (index_is_zdb_index(indexRel) &&
													!ZDBIndexOptionsGetLLAPI(indexRel)) {
													to_drop = lappend(to_drop,
																	  psprintf("%s%s",
																			   ZDBIndexOptionsGetUrl(indexRel),
																			   ZDBIndexOptionsGetIndexName(indexRel)));
												}
												RelationClose(indexRel);
											}
										}
											break;

										default:
											break;
									}


									if (RelationIsValid(rel)) {
										relation_close(rel, AccessExclusiveLock);
									} else {
										UnlockDatabaseObject(address.classId, address.objectId, 0, AccessExclusiveLock);
									}
								}
									break;

								default:
									break;
							}
						}

						MemoryContextSwitchTo(oldContext);
						run_process_utility_hook(parsetree, queryString, context, params, queryEnv, dest,
												 completionTag);
					}
						break;

					default:
						run_process_utility_hook(parsetree, queryString, context, params, queryEnv, dest,
												 completionTag);
						break;
				}
				--executor_depth;
			}
		PG_CATCH();
			{
				--executor_depth;
				PG_RE_THROW();
			}
	PG_END_TRY();

	if (executor_depth == 0) {
		/* cleanup any score and highlight tracking we might have */
		scoring_support_cleanup();
		highlight_support_cleanup();

		currentQueryStack = NULL;
	}
}

ZDBIndexChangeContext *checkout_insert_context(Relation indexRelation, Datum row, bool isnull) {
	ListCell              *lc;
	ZDBIndexChangeContext *context;
	TupleDesc             tupdesc = NULL;

	/* see if we already have an IndexChangeContext for this index */
	foreach (lc, insert_contexts) {
		context = lfirst(lc);
		if (context->indexRelid == RelationGetRelid(indexRelation)) {

			/*
			 * we need to check to see if the TupleDesc that describes
			 * what we're indexing contains a column of type ::json
			 *
			 * ElasticsearchStartBulkProcess() does this too, but we could
			 * have called it with a NULL TupleDesc from our BEFORE UPDATE or DELETE
			 * triggers, and then got here again via an INSERT (or UPDATE) when
			 * we actually know the Datum being indexed
			 */
			if (!isnull && !context->esContext->containsJsonIsSet) {
				context->esContext->containsJson      = datum_contains_json(row);
				context->esContext->containsJsonIsSet = true;
			}

			return context;
		}
	}

	/*
	 * we don't so lets create one and return it
	 */

	if (!isnull)
		tupdesc = lookup_composite_tupdesc(row);

	context = palloc(sizeof(ZDBIndexChangeContext));
	context->indexRelid = RelationGetRelid(indexRelation);
	context->scratch    = AllocSetContextCreate(TopTransactionContext, "aminsert scratch context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
	context->esContext  = ElasticsearchStartBulkProcess(indexRelation, NULL, tupdesc, false);

	insert_contexts = lappend(insert_contexts, context);

	if (tupdesc != NULL)
		ReleaseTupleDesc(tupdesc);

	return context;
}

static void create_delete_trigger(Oid heapRelOid, char *schemaName, char *tableName, Oid indexRelOid) {
	StringInfo sql         = makeStringInfo();
	StringInfo triggerName = makeStringInfo();
	Oid        triggerOid;

	SPI_connect();

	appendStringInfo(triggerName, "zdb_tuple_delete_for_%d_using_%d", heapRelOid, indexRelOid);
	appendStringInfo(sql, "SELECT * FROM pg_trigger WHERE tgname like '%s%%'", triggerName->data);
	if (SPI_execute(sql->data, true, 0) != SPI_OK_SELECT || SPI_processed != 0) {
		SPI_finish();
		return;    /* trigger already exists */
	}

	triggerOid = create_trigger(lookup_zdb_namespace(), schemaName, tableName, heapRelOid, triggerName->data,
								"delete_trigger", indexRelOid, TRIGGER_TYPE_DELETE);
	create_trigger_dependency(indexRelOid, triggerOid);

	freeStringInfo(sql);

	SPI_finish();
}

static void create_update_trigger(Oid heapRelOid, char *schemaName, char *tableName, Oid indexRelOid) {
	StringInfo sql         = makeStringInfo();
	StringInfo triggerName = makeStringInfo();
	Oid        triggerOid;

	SPI_connect();

	appendStringInfo(triggerName, "zdb_tuple_update_for_%d_using_%d", heapRelOid, indexRelOid);
	appendStringInfo(sql, "SELECT * FROM pg_trigger WHERE tgname like '%s%%'", triggerName->data);
	if (SPI_execute(sql->data, true, 0) != SPI_OK_SELECT || SPI_processed != 0) {
		SPI_finish();
		return;    /* trigger already exists */
	}

	triggerOid = create_trigger(lookup_zdb_namespace(), schemaName, tableName, heapRelOid, triggerName->data,
								"update_trigger", indexRelOid, TRIGGER_TYPE_UPDATE);
	create_trigger_dependency(indexRelOid, triggerOid);

	freeStringInfo(sql);

	SPI_finish();
}


void zdb_aminit(void) {
	/* define the GUCs we'll use */
	DefineCustomBoolVariable("zdb.curl_verbose", "Put libcurl into verbose mode", NULL,
							 &zdb_curl_verbose_guc, false, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomEnumVariable("zdb.log_level", "ZomboDB's logging level", NULL, &ZDB_LOG_LEVEL, DEBUG1,
							 zdb_log_level_options, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("zdb.default_elasticsearch_url",
							   "The default Elasticsearch URL ZomboDB should use if not specified on the index", NULL,
							   &zdb_default_elasticsearch_url_guc, NULL, PGC_SIGHUP, 0,
							   validate_default_elasticsearch_url, NULL, NULL);
	DefineCustomIntVariable("zdb.default_row_estimate",
							"The default row estimate ZDB should use", NULL,
							&zdb_default_row_estimation_guc, 2500, -1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("zdb.ignore_visibility", "Should queries honor visibility rules", NULL,
							 &zdb_ignore_visibility_guc, false, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("zdb.default_replicas",
							"The default number of index replicas", NULL,
							&zdb_default_replicas_guc, 0, 0, 32768, PGC_SIGHUP, 0, NULL, NULL, NULL);

	/* define the relation options for use ZDB indexes */
	RELOPT_KIND_ZDB = add_reloption_kind();
	add_string_reloption(RELOPT_KIND_ZDB, "url", "Server URL and port", "default", validate_url);
	add_string_reloption(RELOPT_KIND_ZDB, "type_name",
						 "What Elasticsearch index type name should ZDB use?  Default is 'doc'",
						 "doc", validate_type_name);
	add_string_reloption(RELOPT_KIND_ZDB, "refresh_interval",
						 "Frequency in which Elasticsearch indexes are refreshed.  Related to ES' index.refresh_interval setting",
						 "-1", validate_refresh_interval);
	add_int_reloption(RELOPT_KIND_ZDB, "shards", "The number of shards for the index", 5, 1, 32768);
	add_int_reloption(RELOPT_KIND_ZDB, "replicas", "The number of replicas for the index",
					  zdb_default_replicas_guc, 0, 32768);
	add_int_reloption(RELOPT_KIND_ZDB, "bulk_concurrency", "The maximum number of concurrent _bulk API requests", 12,
					  1, MAX_BULK_CONCURRENCY);
	add_int_reloption(RELOPT_KIND_ZDB, "batch_size", "The size in bytes of batch calls to the _bulk API",
					  1024 * 1024 * 8, 1024, (INT32_MAX / 2) - 1);
	add_int_reloption(RELOPT_KIND_ZDB, "compression_level", "0-9 value to indicate the level of HTTP compression", 1,
					  0, 9);
	add_string_reloption(RELOPT_KIND_ZDB, "alias", "The Elasticsearch Alias to which this index should belong", NULL,
						 validate_alias);
	add_string_reloption(RELOPT_KIND_ZDB, "uuid", "The Elasticsearch index name, as a UUID", NULL, validate_uuid);
	add_int_reloption(RELOPT_KIND_ZDB, "optimize_after",
					  "After how many deleted docs should ZDB _optimize the ES index during VACUUM?", 0, 0, INT32_MAX);
	add_bool_reloption(RELOPT_KIND_ZDB, "llapi", "Will this index be used by ZomboDB's low-level API?", false);

	/* register xact callbacks and planner hooks */
	RegisterXactCallback(xact_commit_callback, NULL);
	RegisterSubXactCallback(subxact_callback, NULL);

	prev_ExecutorStartHook  = ExecutorStart_hook;
	prev_ExecutorEndHook    = ExecutorEnd_hook;
	prev_ExecutorRunHook    = ExecutorRun_hook;
	prev_ExecutorFinishHook = ExecutorFinish_hook;
	prev_ProcessUtilityHook = ProcessUtility_hook;

	ExecutorStart_hook  = zdb_executor_start_hook;
	ExecutorEnd_hook    = zdb_executor_end_hook;
	ExecutorRun_hook    = zdb_executor_run_hook;
	ExecutorFinish_hook = zdb_executor_finish_hook;
	ProcessUtility_hook = zdb_process_utility_hook;
}

Datum zdb_amhandler(PG_FUNCTION_ARGS) {
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies   = 4;
	amroutine->amsupport      = 0;
	amroutine->amcanorder     = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward  = false;
	amroutine->amcanunique    = false;
	amroutine->amcanmulticol  = false;
	amroutine->amoptionalkey  = false;
	amroutine->amsearcharray  = true;
	amroutine->amsearchnulls  = false;
	amroutine->amstorage      = false;
	amroutine->amclusterable  = false;
	amroutine->ampredlocks    = false;
	amroutine->amcanparallel  = false;

	amroutine->amkeytype              = InvalidOid;
	amroutine->amvalidate             = zdbamvalidate;
	amroutine->ambuild                = ambuild;
	amroutine->ambuildempty           = ambuildempty;
	amroutine->aminsert               = aminsert;
	amroutine->ambulkdelete           = ambulkdelete;
	amroutine->amvacuumcleanup        = amvacuumcleanup;
	amroutine->amcanreturn            = NULL;
	amroutine->amcostestimate         = amcostestimate;
	amroutine->amoptions              = amoptions;
	amroutine->amproperty             = NULL;
	amroutine->ambeginscan            = ambeginscan;
	amroutine->amrescan               = amrescan;
	amroutine->amgettuple             = amgettuple;
	amroutine->amgetbitmap            = amgetbitmap;
	amroutine->amendscan              = amendscan;
	amroutine->ammarkpos              = NULL;
	amroutine->amrestrpos             = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan     = NULL;
	amroutine->amparallelrescan       = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*lint -esym 715,opclassoid ignore unused param */
bool zdbamvalidate(Oid opclassoid) {
	return true;
}

/*lint -e533 */
static IndexBuildResult *ambuild(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo) {
	IndexBuildResult  *result    = palloc0(sizeof(IndexBuildResult));
	ZDBBuildStateData buildstate;
	double            reltuples;
	TupleDesc         tupdesc;
	char              *aliasName = ZDBIndexOptionsGetAlias(indexRelation);
	char              *indexName;

	if (ZDBIndexOptionsGetIndexName(indexRelation) != NULL && ZDBIndexOptionsGetLLAPI(indexRelation) == true) {
		/*
		 * this index is a low-level API index that already has a named assigned to it, so we're just going
		 * to return without doing anything.  This is to guard against commands like REINDEX, VACUUM FULL, and
		 * various ALTER statements that rewrite the table and indexes
		 */
		return result;
	}

	if (indexInfo->ii_Expressions == NULL) {
		/* index definitely isn't defined correctly */
		goto definition_error;
	}

	/* index might be defined correctly, we'll validate below */
	tupdesc = extract_tuple_desc_from_index_expressions(indexInfo->ii_Expressions);
	if (tupdesc == NULL)
		goto definition_error;

	/*
	 * Create the remote elasticsearch index
	 */
	if (aliasName == NULL) {
		/* if the user didn't specify an alias, we need to set one */
		aliasName = make_alias_name(indexRelation, false);
		set_index_option(indexRelation, "alias", aliasName);
	}

	indexName = ElasticsearchCreateIndex(heapRelation, indexRelation, tupdesc, aliasName);
	set_index_option(indexRelation, "uuid", indexName);
	ReleaseTupleDesc(tupdesc);

	buildstate.indtuples     = 0;
	buildstate.memoryContext = AllocSetContextCreate(CurrentMemoryContext, "zdbBuildCallback",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
	buildstate.esContext     = ElasticsearchStartBulkProcess(indexRelation, indexName, tupdesc, false);

	/*
	 * Now we insert data into our index
	 */
	reltuples = IndexBuildHeapScan(heapRelation, indexRelation, indexInfo, true, zdbbuildCallback, &buildstate);
	ElasticsearchFinishBulkProcess(buildstate.esContext, true);

	/* Finish up with elasticsearch index creation */
	ElasticsearchFinalizeIndexCreation(indexRelation);

	/*
	 * Attach UPDATE/DELETE triggers so we can track modified rows
	 *
	 * But not for materialized views
	 */
	if (heapRelation->rd_rel->relkind != 'm') {
		create_delete_trigger(RelationGetRelid(heapRelation), get_namespace_name(RelationGetNamespace(heapRelation)),
							  RelationGetRelationName(heapRelation), RelationGetRelid(indexRelation));

		create_update_trigger(RelationGetRelid(heapRelation), get_namespace_name(RelationGetNamespace(heapRelation)),
							  RelationGetRelationName(heapRelation), RelationGetRelid(indexRelation));
	}

	/* Return information about what we just did */
	result->heap_tuples  = reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;

	definition_error:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					errmsg("ZomboDB index definitions must have one column that is a whole row reference to the "
						   "table being indexed (ie, '(table_name.*)' or a function call that returns a "
						   "composite type")));

}

static void ambuildempty(Relation indexRelation) {
	Relation heapRelation = RelationIdGetRelation(IndexGetRelation(RelationGetRelid(indexRelation), false));

	ElasticsearchDeleteIndex(indexRelation);

	RelationClose(heapRelation);
}

/*lint -esym 715,indexRel ignore unused param */
static void zdbbuildCallback(Relation indexRel, HeapTuple htup, Datum *values, bool *isnull, bool tupleIsAlive, void *state) {
	ZDBBuildStateData *buildstate = (ZDBBuildStateData *) state;

	if (!tupleIsAlive)
		return;

	if (HeapTupleIsHeapOnly(htup)) {
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("Heap Only Tuple (HOT) found at (%d, %d).  Run VACUUM FULL <tablename>; and then create the index",
							   ItemPointerGetBlockNumber(&(htup->t_self)),
							   ItemPointerGetOffsetNumber(&(htup->t_self)))));
	}

	if (ZDBIndexOptionsGetLLAPI(indexRel)) {
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("ZomboDB indexes that use its low-level API (llapi=true) must be created on empty tables")));
	}

	if (isnull[0]) {
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("row is null")));
	}

	index_record(buildstate->esContext, buildstate->memoryContext, &htup->t_self, values[0], htup);
	buildstate->indtuples++;
}


/*lint -esym 715,heapRelation,checkUnique,indexInfo ignore unused param */
static bool aminsert(Relation indexRelation, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRelation, IndexUniqueCheck checkUnique, IndexInfo *indexInfo) {
	ZDBIndexChangeContext *insertContext;
	MemoryContext         oldContext;

	if (ZDBIndexOptionsGetLLAPI(indexRelation)) {
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("ZomboDB indexes that use its low-level API (llapi=true) cannot be directly inserted into"),
						errhint("You probably want to call SELECT zdb.llapi_direct_insert() instead")));
	}

	if (isnull[0]) {
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("row is null")));
	}

	/*
	 * when actually indexing the record (sending it to Elasticsearch) we need to be in a MemoryContext
	 * that's still active when we want to call ElasticsearchFinishBulkProcess().
	 */
	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	insertContext = checkout_insert_context(indexRelation, values[0], isnull[0]);

	index_record(insertContext->esContext, insertContext->scratch, heap_tid, values[0], NULL);
	MemoryContextSwitchTo(oldContext);

	return true;
}

static void index_record(ElasticsearchBulkContext *esContext, MemoryContext scratchContext, ItemPointer ctid, Datum record, HeapTuple htup) {
	MemoryContext oldContext;
	text          *json;
	CommandId     cmin;
	CommandId     cmax;
	uint64        xmin;
	uint64        xmax;

	/*
	 * create the json form of the input record in the specified MemoryContext
	 * and then switch back to whatever the current context is
	 */
	oldContext = MemoryContextSwitchTo(scratchContext);
	json       = DatumGetTextP(DirectFunctionCall1(row_to_json, record));
	MemoryContextSwitchTo(oldContext);

	if (htup == NULL) {
		/* it's from an INSERT or UPDATE statement */
		cmin = GetCurrentCommandId(true);
		cmax = cmin;

		xmin = convert_xid(GetCurrentTransactionId());
		xmax = InvalidTransactionId;
	} else {
		/* it's from a CREATE INDEX statement */
		cmin = HeapTupleHeaderGetRawCommandId(htup->t_data);
		cmax = HeapTupleHeaderGetRawCommandId(htup->t_data);

		xmin = convert_xid(HeapTupleHeaderGetXmin(htup->t_data));
		xmax = InvalidTransactionId;
	}

	/* add the row to Elasticsearch */
	ElasticsearchBulkInsertRow(esContext, ctid, json, cmin, cmax, xmin, xmax);

	/*
	 * and now that we've used the json value, free the MemoryContext in which it was allocated.
	 *
	 * if we don't do this, we'll leak the json string version of every row being indexed
	 * until the transaction ends!
	 */
	MemoryContextResetAndDeleteChildren(scratchContext);
}

static IndexBulkDeleteResult *zdb_vacuum_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, bool via_cleanup) {
	static char      *zdb_x_fields[]       = {"zdb_xmin", "zdb_xmax"};
	static char      *zdb_aborted_fields[] = {"zdb_aborted_xids"};
	static const Oid args[]                = {REGCLASSOID, TEXTOID, INT8OID};
	Oid              byXmin;
	Oid              byXmax;
	Oid              byAbtXmax;
	bool             savedIgnoreVisibility = zdb_ignore_visibility_guc;

	IndexBulkDeleteResult *result = palloc0(sizeof(IndexBulkDeleteResult));
	TransactionId         oldestXmin;
	ZDBQueryType          *query;

	byXmin    = LookupFuncName(lappend(lappend(NIL, makeString("zdb")), makeString("vac_by_xmin")), 3, args, false);
	byXmax    = LookupFuncName(lappend(lappend(NIL, makeString("zdb")), makeString("vac_by_xmax")), 3, args, false);
	byAbtXmax = LookupFuncName(lappend(lappend(NIL, makeString("zdb")), makeString("vac_aborted_xmax")), 3, args,
							   false);

	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	oldestXmin = TransactionIdLimitedForOldSnapshots(GetOldestXmin(info->index, PROCARRAY_FLAGS_VACUUM), info->index);

	PG_TRY();
			{
				ElasticsearchScrollContext *scroll;
				ElasticsearchBulkContext   *bulk;
				int                        deleted = 0, xmaxes_reset = 0;

				zdb_ignore_visibility_guc = true;

				if (strcmp(ZDBIndexOptionsGetRefreshInterval(info->index), "-1") != 0) {
					/*
					 * The index has a custom refresh interval, but VACUUM needs a refreshed index
					 * so we'll force one now
					 */
					char *tmp;

					tmp = ElasticsearchArbitraryRequest(info->index, "POST", "_refresh", NULL);
					pfree(tmp);
				}

				bulk = ElasticsearchStartBulkProcess(info->index, NULL, NULL, true);

				/*
				 * Find all rows with what we think is an *aborted* xmin
				 *
				 * These rows can be deleted
				 */
				query  = (ZDBQueryType *) DatumGetPointer(
						OidFunctionCall3(byXmin,
										 ObjectIdGetDatum(RelationGetRelid(info->index)),
										 CStringGetTextDatum(ZDBIndexOptionsGetTypeName(info->index)),
										 Int64GetDatum(convert_xid(oldestXmin))));
				scroll = ElasticsearchOpenScroll(info->index, query, true, false, false, 0, NULL, SORTBY_DEFAULT, NULL,
												 zdb_x_fields, 2);
				while (scroll->cnt < scroll->total) {
					char          *_id;
					TransactionId xmin;

					ElasticsearchGetNextItemPointer(scroll, NULL, &_id, NULL, NULL);
					xmin = (TransactionId) get_json_first_array_uint64(scroll->fields, "zdb_xmin");

					if (TransactionIdPrecedes(xmin, oldestXmin) && TransactionIdDidAbort(xmin) &&
						!TransactionIdDidCommit(xmin) && !TransactionIdIsInProgress(xmin)) {
						ElasticsearchBulkDeleteRowByXmin(bulk, _id, convert_xid(xmin));
						deleted++;
					}
				}
				ElasticsearchCloseScroll(scroll);

				/*
				 * Find all rows with what we think is a *committed* xmax
				 *
				 * These rows can be deleted
				 */
				query  = (ZDBQueryType *) DatumGetPointer(
						OidFunctionCall3(byXmax,
										 ObjectIdGetDatum(RelationGetRelid(info->index)),
										 CStringGetTextDatum(ZDBIndexOptionsGetTypeName(info->index)),
										 Int64GetDatum(convert_xid(oldestXmin))));
				scroll = ElasticsearchOpenScroll(info->index, query, true, false, false, 0, NULL, SORTBY_DEFAULT, NULL,
												 zdb_x_fields, 2);
				while (scroll->cnt < scroll->total) {
					char          *_id;
					TransactionId xmax;

					ElasticsearchGetNextItemPointer(scroll, NULL, &_id, NULL, NULL);
					xmax = (TransactionId) get_json_first_array_uint64(scroll->fields, "zdb_xmax");

					if (TransactionIdPrecedes(xmax, oldestXmin) && TransactionIdDidCommit(xmax) &&
						!TransactionIdDidAbort(xmax) && !TransactionIdIsInProgress(xmax)) {
						ElasticsearchBulkDeleteRowByXmax(bulk, _id, convert_xid(xmax));
						deleted++;
					}
				}
				ElasticsearchCloseScroll(scroll);

				/*
				 * Find all rows with what we think is an *aborted* xmax
				 *
				 * These rows can have their xmax reset to null because they're still live
				 */
				query  = (ZDBQueryType *) DatumGetPointer(
						OidFunctionCall3(byAbtXmax,
										 ObjectIdGetDatum(RelationGetRelid(info->index)),
										 CStringGetTextDatum(ZDBIndexOptionsGetTypeName(info->index)),
										 Int64GetDatum(convert_xid(oldestXmin))));
				scroll = ElasticsearchOpenScroll(info->index, query, true, false, false, 0, NULL, SORTBY_DEFAULT, NULL,
												 zdb_x_fields, 2);
				while (scroll->cnt < scroll->total) {
					char          *_id;
					TransactionId xmax;
					uint64        xmax64;

					ElasticsearchGetNextItemPointer(scroll, NULL, &_id, NULL, NULL);
					xmax64 = get_json_first_array_uint64(scroll->fields, "zdb_xmax");
					xmax   = (TransactionId) xmax64;

					if (TransactionIdPrecedes(xmax, oldestXmin) && TransactionIdDidAbort(xmax) &&
						!TransactionIdDidCommit(xmax) && !TransactionIdIsInProgress(xmax)) {
						ElasticsearchBulkVacuumXmax(bulk, _id, xmax64);
						xmaxes_reset++;
					}
				}
				ElasticsearchCloseScroll(scroll);

				/* finish the bulk process for vacuuming */
				ElasticsearchFinishBulkProcess(bulk, true);

				/*
				 * Finally, any "zdb_aborted_xid" value we have can be removed if it's
				 * known to be aborted and no longer referenced anywhere in the index
				 */
				scroll = ElasticsearchOpenScroll(info->index, MakeZDBQuery("_id:zdb_aborted_xids"), true, false, false,
												 0, NULL, SORTBY_DEFAULT, NULL, zdb_aborted_fields, 1);
				while (scroll->cnt < scroll->total) {
					void *array;

					ElasticsearchGetNextItemPointer(scroll, NULL, NULL, NULL, NULL);
					if (scroll->fields == NULL)
						continue;

					array = get_json_object_array(scroll->fields, "zdb_aborted_xids", true);

					if (array != NULL) {
						List *to_remove = NIL;
						int  i, len     = get_json_array_length(array);

						for (i = 0; i < len; i++) {
							uint64        xid64 = get_json_array_element_uint64(array, i, scroll->jsonMemoryContext);
							TransactionId xid   = (TransactionId) xid64;

							if (TransactionIdPrecedes(xid, oldestXmin) && TransactionIdDidAbort(xid) &&
								!TransactionIdDidCommit(xid) && !TransactionIdIsInProgress(xid)) {
								uint64 xmin_cnt = ElasticsearchCount(info->index,
																	 MakeZDBQuery(psprintf("zdb_xmin:%lu", xid64)));
								uint64 xmax_cnt = ElasticsearchCount(info->index,
																	 MakeZDBQuery(psprintf("zdb_xmax:%lu", xid64)));

								/* if it's not referenced anywhere, so we can remove it */
								if (xmin_cnt == 0 && xmax_cnt == 0) {
									uint64 *tmp = palloc(sizeof(uint64));
									memcpy(tmp, &xid64, sizeof(uint64));

									to_remove = lappend(to_remove, tmp);
								}
							}
						}
						ElasticsearchRemoveAbortedTransactions(info->index, to_remove);

						if (list_length(to_remove) > 0)
							elog(LOG, "[zombodb-vacuum] removed %d aborted xids", list_length(to_remove));
					}
				}
				ElasticsearchCloseScroll(scroll);


				stats->tuples_removed   = deleted;
				stats->num_index_tuples = ElasticsearchCount(info->index, MakeZDBQuery(""));

				if (deleted > 0 || xmaxes_reset > 0) {
					elog(LOG, "[zombodb-vacuum] deleted=%d, xmax_reset=%d, via_cleanup=%s", deleted, xmaxes_reset,
						 via_cleanup ? "true" : "false");
				}

				zdb_ignore_visibility_guc = savedIgnoreVisibility;
			}
		PG_CATCH();
			{
				zdb_ignore_visibility_guc = savedIgnoreVisibility;
				PG_RE_THROW();
			}
	PG_END_TRY();

	return result;
}

/*lint -esym 715,callback,callback_state ignore unused params */
static IndexBulkDeleteResult *ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state) {
	return zdb_vacuum_internal(info, stats, false);
}

static IndexBulkDeleteResult *amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats) {

	if (stats == NULL) {
		stats = zdb_vacuum_internal(info, stats, true);
	}

	return stats;
}

/*lint -esym 715,root,loop_count ignore unused param */
static void amcostestimate(struct PlannerInfo *root, struct IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation, double *indexPages) {
	Relation indexRel = RelationIdGetRelation(path->indexinfo->indexoid);
	Relation heapRel  = RelationIdGetRelation(IndexGetRelation(RelationGetRelid(indexRel), false));
	bool     isset    = false;
	ListCell *lc;

	/*
	 * we subtract random_page_cost from the total cost because Postgres assumes we'll read at least
	 * one index page, and that's just not true for ZomboDB -- we have no pages on disk
	 *
	 * Assuming default values for random_page_cost and seq_page_cost, this should always
	 * get our IndexScans set to a lower cost than a sequential scan, which we don't necessarily prefer,
	 * allowing Postgres to instead prefer to use our index for plans where it can actually use one
	 */
	if (*indexTotalCost > random_page_cost)
		*indexTotalCost -= random_page_cost;

	/*
	 * go with the smallest already-calculated selectivity.
	 *
	 * these would have been calculated in zdb_restrict()
	 */
	foreach(lc, path->indexclauses) {
		RestrictInfo *ri = lfirst(lc);

		if (ri->norm_selec > -1) {
			if (!isset) {
				*indexSelectivity = ri->norm_selec;
				isset = true;
			} else {
				*indexSelectivity = Min(ri->norm_selec, *indexSelectivity);
			}
		}
	}

	*indexStartupCost = 0;
	*indexCorrelation = 1;    /* because an IndexScan will sort by zdb_ctid in ES, which will give us heap order */
	*indexPages       = 0;

	*indexTotalCost += (*indexSelectivity * Max(1, heapRel->rd_rel->reltuples)) * (cpu_index_tuple_cost);

	RelationClose(heapRel);
	RelationClose(indexRel);
}

static bytea *amoptions(Datum reloptions, bool validate) {
	relopt_value                  *options;
	ZDBIndexOptions               *rdopts;
	int                           numoptions;
	int                           i;
	static const relopt_parse_elt tab[] = {
			{"url",               RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, urlValueOffset)},
			{"type_name",         RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, typeNameValueOffset)},
			{"refresh_interval",  RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, refreshIntervalOffset)},
			{"shards",            RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, shards)},
			{"replicas",          RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, replicas)},
			{"bulk_concurrency",  RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, bulk_concurrency)},
			{"batch_size",        RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, batch_size)},
			{"compression_level", RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, compressionLevel)},
			{"alias",             RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, aliasOffset)},
			{"optimize_after",    RELOPT_TYPE_INT,    offsetof(ZDBIndexOptions, optimizeAfter)},
			{"llapi",             RELOPT_TYPE_BOOL,   offsetof(ZDBIndexOptions, llapi)},
			{"uuid",              RELOPT_TYPE_STRING, offsetof(ZDBIndexOptions, uuidOffset)},
	};

	options = parseRelOptions(reloptions, validate, RELOPT_KIND_ZDB, &numoptions);

	/* if none set, we're done */
	if (numoptions == 0)
		return NULL;

	/* set the lock mode for all the options */
	for (i = 0; i < numoptions; i++) {
		options[i].gen->lockmode = AccessShareLock;
	}

	rdopts = allocateReloptStruct(sizeof(ZDBIndexOptions), options, numoptions);

	fillRelOptions((void *) rdopts, sizeof(ZDBIndexOptions), options, numoptions, validate, tab, lengthof(tab));

	pfree(options);

	return (bytea *) rdopts;
}

static float4 scoring_cb(ItemPointer ctid, void *arg) {
	ZDBScanContext *context = (ZDBScanContext *) arg;

	assert(ctid != NULL);

	if (ItemPointerIsValid(&context->lastCtid) && ItemPointerEquals(ctid, &context->lastCtid))
		return context->lastScore;

	if (context->scoreLookup != NULL) {
		ZDBScoreKey   key;
		ZDBScoreEntry *entry;
		bool          found;

		ItemPointerCopy(ctid, &key.ctid);
		entry = hash_search(context->scoreLookup, &key, HASH_FIND, &found);
		if (entry != NULL && found)
			return entry->score;
	}

	return 0;
}

static List *highlight_cb(ItemPointer ctid, Name field, void *arg) {
	ZDBScanContext *context = (ZDBScanContext *) arg;

	assert(ctid != NULL);
	assert(field != NULL);

	if (context->highlightLookup != NULL) {
		ZDBHighlightKey   key;
		ZDBHighlightEntry *entry;
		bool              found;

		memset(&key, 0, sizeof(ZDBHighlightKey));
		ItemPointerCopy(ctid, &key.ctid);
		memcpy(&key.field, field, sizeof(NameData));

		entry = hash_search(context->highlightLookup, &key, HASH_FIND, &found);
		if (entry != NULL && found)
			return entry->highlights;
	}

	return NULL;
}

static IndexScanDesc ambeginscan(Relation indexRelation, int nkeys, int norderbys) {
	IndexScanDesc  scan = RelationGetIndexScan(indexRelation, nkeys, norderbys);
	ZDBScanContext *context;

	context = palloc0(sizeof(ZDBScanContext));

	scan->xs_itupdesc = RelationGetDescr(indexRelation);
	scan->opaque      = context;

	return scan;
}

static inline void do_search_for_scan(IndexScanDesc scan) {
	ZDBScanContext *context = (ZDBScanContext *) scan->opaque;

	if (context->needsInit) {
		Relation  heapRel    = scan->heapRelation;
		char      *sortField;
		SortByDir sortdir    = SORTBY_DEFAULT;
		uint64    limit      = 0;
		bool      wantScores;
		List      *highlights;

		if (scan->heapRelation == NULL)
			heapRel = RelationIdGetRelation(IndexGetRelation(RelationGetRelid(scan->indexRelation), false));

		wantScores = current_scan_wants_scores(scan, heapRel);
		highlights = extract_highlight_info(scan, RelationGetRelid(heapRel));
		sortField  = find_sort_and_limit_for_scan(scan, &sortdir, &limit);

		if (limit == 0)
			limit = find_limit_for_scan(scan);

		if (context->scrollContext != NULL) {
			ElasticsearchCloseScroll(context->scrollContext);
		}

		context->scrollContext  = ElasticsearchOpenScroll(scan->indexRelation, context->query, false, true, wantScores,
														  limit,
														  sortField, sortdir, highlights, NULL, 0);
		context->wantHighlights = highlights != NULL;
		context->wantScores     = wantScores;
		if (context->wantScores) {
			context->scoreLookup = scoring_create_lookup_table(TopTransactionContext, "bitmap scores");
			scoring_register_callback(RelationGetRelid(heapRel), scoring_cb, context, CurrentMemoryContext);
		}

		if (context->wantHighlights) {
			context->highlightLookup = highlight_create_lookup_table(TopTransactionContext, "highlights");
			highlight_register_callback(RelationGetRelid(heapRel), highlight_cb, context, CurrentMemoryContext);
		}

		if (scan->heapRelation == NULL)
			RelationClose(heapRel);

		context->needsInit = false;
	}
}

/*lint -esym 715,orderbys,norderbys ignore unused param */
static void amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys) {
	ZDBScanContext *context = (ZDBScanContext *) scan->opaque;

	context->query         = scan_keys_to_query_dsl(keys, nkeys);
	context->needsInit     = true;
}

/*lint -esym 715,direction ignore unused param */
static bool amgettuple(IndexScanDesc scan, ScanDirection direction) {
	ZDBScanContext  *context = (ZDBScanContext *) scan->opaque;
	zdb_json_object highlights;

	do_search_for_scan(scan);

	/* zdb indexes are never lossy */
	scan->xs_recheck = false;

	if (context->scrollContext->limit > 0 && context->scrollContext->limitcnt >= context->scrollContext->limit)
		return false; /* we've reached our limit of live tuples */
	else if (context->scrollContext->cnt >= context->scrollContext->total)
		return false; /* we have no more tuples to return */

	/* get the next tuple from Elasticsearch */
	ElasticsearchGetNextItemPointer(context->scrollContext, &context->lastCtid, NULL, &context->lastScore, &highlights);
	if (!ItemPointerIsValid(&context->lastCtid))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Encounted an invalid item pointer: (%d, %d)",
							   ItemPointerGetBlockNumber(&context->lastCtid),
							   ItemPointerGetOffsetNumber(&context->lastCtid))));

	/* tell the index scan about the tuple we're going to return */
	ItemPointerCopy(&context->lastCtid, &scan->xs_ctup.t_self);

	/*
	 * If we're operating within a LIMIT, we need to ensure the rows we count toward that LIMIT
	 * are actually visible within our current snapshot, so we go to the underlying heap and
	 * try to fetch the tuple we're about to return.  If it's found, then we count it towards
	 * our limit.
	 */
	if (context->scrollContext->limit > 0) {
		HeapTupleData tuple;
		Buffer        buf;

		ItemPointerCopy(&context->lastCtid, &tuple.t_self);

		if (heap_fetch(scan->heapRelation, scan->xs_snapshot, &tuple, &buf, false, scan->indexRelation)) {
			/* this counts towards our limit */
			context->scrollContext->limitcnt++;
		}

		if (BufferIsValid(buf))
			ReleaseBuffer(buf);
	}

	if (context->wantScores) {
		ZDBScoreKey   key;
		ZDBScoreEntry *entry;
		bool          found;

		/*
		 * track scores in our hashtable too
		 * This is necessary if we are doing scoring but our IndexScan
		 * is under, at least, a Sort node
		 */
		ItemPointerCopy(&context->lastCtid, &key.ctid);
		entry = hash_search(context->scoreLookup, &key, HASH_ENTER, &found);
		ItemPointerCopy(&context->lastCtid, &entry->key.ctid);
		entry->score = context->lastScore;
	}

	if (context->wantHighlights) {
		save_highlights(context->highlightLookup, &context->lastCtid, highlights);
	}

	return true;
}

static int64 amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm) {
	ZDBScanContext *context = (ZDBScanContext *) scan->opaque;
	int64          ntuples  = 0;

	do_search_for_scan(scan);

	while (context->scrollContext->cnt < context->scrollContext->total) {
		ItemPointerData ctid;
		float4          score;
		ZDBScoreKey     key;
		ZDBScoreEntry   *entry;
		bool            found;

		ElasticsearchGetNextItemPointer(context->scrollContext, &ctid, NULL, &score, NULL);

		ItemPointerCopy(&ctid, &key.ctid);

		if (context->wantScores) {
			entry = hash_search(context->scoreLookup, &key, HASH_ENTER, &found);
			ItemPointerCopy(&ctid, &entry->key.ctid);
			entry->score = score;
		}

		tbm_add_tuples(tbm, &ctid, 1, false);
		ntuples++;
	}

	return ntuples;
}

static void amendscan(IndexScanDesc scan) {
	ZDBScanContext *context = (ZDBScanContext *) scan->opaque;

	if (context->query != NULL)
		pfree(context->query);

	if (context->scrollContext != NULL)
		ElasticsearchCloseScroll(context->scrollContext);

	if (context->scoreLookup != NULL)
		hash_destroy(context->scoreLookup);

	if (context->highlightLookup != NULL)
		hash_destroy(context->highlightLookup);

	pfree(scan->opaque);
}

static void handle_trigger(Oid indexRelId, ItemPointer targetCtid) {
	MemoryContext         oldContext;
	ZDBIndexChangeContext *context;
	Relation              indexRel;

	oldContext = MemoryContextSwitchTo(TopTransactionContext);
	indexRel   = zdb_open_index(indexRelId, AccessShareLock);

	context = checkout_insert_context(indexRel, PointerGetDatum(NULL), true);

	ElasticsearchBulkUpdateTuple(context->esContext, targetCtid, NULL, GetCurrentCommandId(true),
								 convert_xid(GetCurrentTransactionId()));

	relation_close(indexRel, AccessShareLock);
	MemoryContextSwitchTo(oldContext);
}

Datum zdb_delete_trigger(PG_FUNCTION_ARGS) {
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Oid         indexRelId;

	/* make sure it's called as a trigger at all */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "zdb_delete_trigger: not called by trigger manager");

	if (!(TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)))
		elog(ERROR, "zdb_delete_trigger: can only be fired for DELETE triggers");

	if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
		elog(ERROR, "zdb_delete_trigger: can only be fired as a BEFORE trigger");

	if (trigdata->tg_trigger->tgnargs != 1)
		elog(ERROR, "zdb_delete_trigger: called with incorrect number of arguments");

	indexRelId = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(trigdata->tg_trigger->tgargs[0])));
	handle_trigger(indexRelId, &trigdata->tg_trigtuple->t_self);

	return PointerGetDatum(trigdata->tg_trigtuple);
}

Datum zdb_update_trigger(PG_FUNCTION_ARGS) {
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Oid         indexRelId;

	/* make sure it's called as a trigger at all */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "zdb_update_trigger: not called by trigger manager");

	if (!(TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)))
		elog(ERROR, "zdb_update_trigger: can only be fired for UPDATE triggers");

	if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
		elog(ERROR, "zdb_update_trigger: can only be fired as a BEFORE trigger");

	if (trigdata->tg_trigger->tgnargs != 1)
		elog(ERROR, "zdb_update_trigger: called with incorrect number of arguments");

	indexRelId = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(trigdata->tg_trigger->tgargs[0])));
	handle_trigger(indexRelId, &trigdata->tg_trigtuple->t_self);

	return PointerGetDatum(trigdata->tg_newtuple);
}

static void apply_alter_statement(PlannedStmt *parsetree, char *url, uint32 shards, char *typeName, char *oldAlias, char *oldUUID) {
	LOCKMODE lockmode = AccessShareLock;
	if (shards > 0) {
		/*
		 * it's specifically a ZomboDB index, so lets detect if the number of shards changed to emit
		 * a warning, and also push any changed index settings to the backing ES index
		 */
		char     *newurl;
		uint32   newshards;
		char     *newTypeName;
		char     *newalias;
		char     *newuuid;
		bool     is_index;
		Relation rel;

		rel = open_relation_from_parsetree(parsetree, lockmode, &is_index);

		get_immutable_index_options(parsetree, &newurl, &newshards, &newTypeName, &newalias, &newuuid);

		if (strcmp(url, newurl) != 0) {
			if (!session_auth_is_superuser)
				elog(ERROR, "You must be a superuser to change the 'url' parameter");
		}

		if (strcmp(typeName, newTypeName) != 0) {
			elog(ERROR, "The 'type_name' index properly can only be set during CREATE INDEX");
		}

		if (strcmp(oldUUID, newuuid) != 0) {
			elog(ERROR, "The 'uuid' index property cannot be changed");
		}

		if (shards != newshards) {
			elog(WARNING,
				 "Number of shards changed from %d to %d.  You must issue a REINDEX before this change will take effect",
				 shards, newshards);
		}

		if (newalias == NULL) {
			/* if the user tried to reset the alias instead switch it to our default */
			newalias = make_alias_name(rel, true);
			set_index_option(rel, "alias", newalias);
		}

		ElasticsearchUpdateSettings(rel, oldAlias, newalias);
		relation_close(rel, lockmode);
	} else {
		bool     is_index;
		Relation heapRel;

		heapRel = open_relation_from_parsetree(parsetree, lockmode, &is_index);
		if (RelationIsValid(heapRel)) {
			/*
			 * it's a table, so lets see if it has any ZDB indexes attached to it, and update the mapping
			 * definitions for each one
			 */
			ListCell *lc;

			foreach (lc, RelationGetIndexList(heapRel)) {
				Oid      relid = lfirst_oid(lc);
				Relation indexRel;

				indexRel = relation_open(relid, lockmode);
				if (index_is_zdb_index(indexRel)) {
					TupleDesc tupdesc;

					tupdesc = extract_tuple_desc_from_index_expressions(RelationGetIndexExpressions(indexRel));
					ElasticsearchPutMapping(heapRel, indexRel, tupdesc);
					ReleaseTupleDesc(tupdesc);
				}
				relation_close(indexRel, lockmode);
			}
			relation_close(heapRel, lockmode);
		}
	}
}

static Relation open_relation_from_parsetree(PlannedStmt *parsetree, LOCKMODE lockmode, bool *is_index) {
	if (IsA(parsetree->utilityStmt, AlterTableStmt)) {
		AlterTableStmt *stmt = (AlterTableStmt *) parsetree->utilityStmt;
		Oid            relid;
		Relation       rel;

		relid = AlterTableLookupRelation(stmt, lockmode);

		rel = relation_open(relid, lockmode);
		switch (stmt->relkind) {
			case OBJECT_INDEX:
				if (index_is_zdb_index(rel)) {
					*is_index = true;
					return rel;
				}
				break;

			case OBJECT_TABLE:
				*is_index = false;
				return rel;

			default:
				break;
		}
		relation_close(rel, lockmode);
	}

	*is_index = false;
	return NULL;
}

static void get_immutable_index_options(PlannedStmt *parsetree, char **url, uint32 *shards, char **typeName, char **alias, char **uuid) {
	LOCKMODE lockmode = NoLock;
	Relation rel;
	bool     is_index;

	*url      = NULL;
	*shards   = 0;
	*typeName = NULL;
	*alias    = NULL;
	*uuid     = NULL;

	rel = open_relation_from_parsetree(parsetree, AccessShareLock, &is_index);
	if (RelationIsValid(rel)) {
		if (is_index) {
			*url      = ZDBIndexOptionsGetUrl(rel);
			*shards   = ZDBIndexOptionsGetNumberOfShards(rel);
			*typeName = pstrdup(ZDBIndexOptionsGetTypeName(rel));
			*alias    = ZDBIndexOptionsGetAlias(rel) != NULL ? pstrdup(ZDBIndexOptionsGetAlias(rel)) : NULL;
			*uuid     = pstrdup(ZDBIndexOptionsGetIndexName(rel));
		}

		relation_close(rel, lockmode);
	}
}
