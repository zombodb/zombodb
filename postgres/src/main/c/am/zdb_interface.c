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
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "commands/dbcommands.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "zdb_interface.h"
#include "util/zdbutils.h"
#include "elasticsearch.h"

relopt_kind RELOPT_KIND_ZDB;

static void wrapper_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, bool noxact, char *fieldProperties);
static void wrapper_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor);
static void wrapper_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping);

static void wrapper_dropIndex(ZDBIndexDescriptor *indexDescriptor);
static void wrapper_refreshIndex(ZDBIndexDescriptor *indexDescriptor);

static uint64 			  wrapper_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name);
static uint64             wrapper_estimateCount(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries);
static uint64             wrapper_estimateSelectivity(ZDBIndexDescriptor *indexDescriptor, char *query);
static ZDBSearchResponse *wrapper_searchIndex(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries, uint64 *nhits);
static ZDBSearchResponse *wrapper_getPossiblyExpiredItems(ZDBIndexDescriptor *indexDescriptor, uint64 *nitems);

static char *wrapper_tally(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order);
static char *wrapper_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *range_spec, char *query);
static char *wrapper_significant_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms);
static char *wrapper_extended_stats(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *user_query);
static char *wrapper_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *aggregate_query, char *user_query);
static char *wrapper_suggest_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms);

static char *wrapper_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname);
static char *wrapper_getIndexMapping(ZDBIndexDescriptor *indexDescriptor);

static char *wrapper_highlight(ZDBIndexDescriptor *indexDescriptor, char *query, char *documentJson);

static void wrapper_freeSearchResponse(ZDBSearchResponse *searchResponse);

static void wrapper_bulkDelete(ZDBIndexDescriptor *indexDescriptor, List *itemPointers, int nitems);

static void wrapper_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, TransactionId xmin, TransactionId xmax, CommandId cmin, CommandId cmax, bool xmin_is_committed, bool xmax_is_committed, text *data);
static void wrapper_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor);

static void wrapper_commitXactData(ZDBIndexDescriptor *indexDescriptor, List *xactData);

static void wrapper_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType);

static void validate_url(char *str)
{
	if (str && str[strlen(str) - 1] != '/')
		elog(ERROR, "'url' index option must end in a slash");
}

static void validate_shadow(char *str)
{
	if (str && DatumGetObjectId(DirectFunctionCall1(regclassin, PointerGetDatum(str))) == InvalidOid)
		elog(ERROR, "Invalid shadow index name: %s", str);
}

static void validate_options(char *str)
{
	// TODO:  implement this
}

static void validate_preference(char *str) {
	// noop
}

static void validate_field_lists(char *str) {
	// TODO:  implement this
}

void zdb_index_init(void)
{
	RELOPT_KIND_ZDB = add_reloption_kind();
	add_string_reloption(RELOPT_KIND_ZDB, "url", "Server URL and port", NULL, validate_url);
	add_string_reloption(RELOPT_KIND_ZDB, "shadow", "A zombodb index to which this one should shadow", NULL, validate_shadow);
	add_string_reloption(RELOPT_KIND_ZDB, "options", "Comma-separated list of options to pass to underlying index", NULL, validate_options);
	add_string_reloption(RELOPT_KIND_ZDB, "preference", "The ?preference value used to Elasticsearch", NULL, validate_preference);
	add_int_reloption(RELOPT_KIND_ZDB, "shards", "The number of shared for the index", 5, 1, 32768);
	add_int_reloption(RELOPT_KIND_ZDB, "replicas", "The default number of replicas for the index", 1, 1, 32768);
	add_bool_reloption(RELOPT_KIND_ZDB, "noxact", "Disable transaction tracking for this index", false);
	add_int_reloption(RELOPT_KIND_ZDB, "bulk_concurrency", "The maximum number of concurrent _bulk API requests", 12, 1, 12);
	add_int_reloption(RELOPT_KIND_ZDB, "batch_size", "The size in bytes of batch calls to the _bulk API", 1024*1024*8, 1024, 1024*1024*64);
	add_string_reloption(RELOPT_KIND_ZDB, "field_lists", "field=[field1, field2, field3], other=[field4,field5]", NULL, validate_field_lists);
}

ZDBIndexDescriptor *zdb_alloc_index_descriptor(Relation indexRel)
{
	MemoryContext      oldContext = MemoryContextSwitchTo(TopTransactionContext);
	StringInfo         scratch    = makeStringInfo();
	Relation           heapRel;
	ZDBIndexDescriptor *desc;

	if (indexRel->rd_index == NULL)
		elog(ERROR, "%s is not an index", RelationGetRelationName(indexRel));

	heapRel = relation_open(indexRel->rd_index->indrelid, AccessShareLock);

	desc = palloc0(sizeof(ZDBIndexDescriptor));

	/* these all come from the actual index */
	desc->indexRelid     = RelationGetRelid(indexRel);
	desc->isShadow		 = ZDBIndexOptionsGetShadow(indexRel) != NULL;
	desc->logit			 = false;
	desc->databaseName   = pstrdup(get_database_name(MyDatabaseId));
	desc->schemaName     = pstrdup(get_namespace_name(RelationGetNamespace(heapRel)));
	desc->tableName      = pstrdup(RelationGetRelationName(heapRel));
	desc->options		 = ZDBIndexOptionsGetOptions(indexRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetOptions(indexRel));

	desc->searchPreference = ZDBIndexOptionsGetSearchPreference(indexRel);
	desc->bulk_concurrency = ZDBIndexOptionsGetBulkConcurrency(indexRel);
	desc->batch_size       = ZDBIndexOptionsGetBatchSize(indexRel);
	desc->fieldLists       = ZDBIndexOptionsGetFieldLists(indexRel);

	if (desc->isShadow)
	{
		/* but some properties come from the index we're shadowing */
		Oid shadowRelid = DatumGetObjectId(DirectFunctionCall1(regclassin, PointerGetDatum(ZDBIndexOptionsGetShadow(indexRel))));
		Relation shadowRel;

		if (shadowRelid == InvalidOid)
			elog(ERROR, "No such shadow index: %s", ZDBIndexOptionsGetShadow(indexRel));

		shadowRel = relation_open(shadowRelid, AccessShareLock);
		desc->advisory_mutex = (int64) oid_hash(&shadowRelid, sizeof(Oid));
		desc->indexName      = pstrdup(RelationGetRelationName(shadowRel));
		desc->url            = ZDBIndexOptionsGetUrl(shadowRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetUrl(shadowRel));
		relation_close(shadowRel, AccessShareLock);
	}
	else
	{
		/* or just from the actual index if we're not a shadow */
		desc->advisory_mutex = (int64) oid_hash(&desc->indexRelid, sizeof(Oid));
		desc->indexName      = pstrdup(RelationGetRelationName(indexRel));
		desc->url            = ZDBIndexOptionsGetUrl(indexRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetUrl(indexRel));
	}

	appendStringInfo(scratch, "%s.%s.%s.%s", desc->databaseName, desc->schemaName, desc->tableName, desc->indexName);
	desc->fullyQualifiedName = pstrdup(scratch->data);

	resetStringInfo(scratch);
	appendStringInfo(scratch, "%s.%s", get_namespace_name(RelationGetNamespace(heapRel)), RelationGetRelationName(heapRel));
	desc->qualifiedTableName = pstrdup(scratch->data);

	desc->implementation                       = palloc0(sizeof(ZDBIndexImplementation));
	desc->implementation->createNewIndex       = wrapper_createNewIndex;
	desc->implementation->finalizeNewIndex     = wrapper_finalizeNewIndex;
	desc->implementation->updateMapping		   = wrapper_updateMapping;
	desc->implementation->dropIndex            = wrapper_dropIndex;
	desc->implementation->refreshIndex         = wrapper_refreshIndex;
	desc->implementation->actualIndexRecordCount = wrapper_actualIndexRecordCount;
	desc->implementation->estimateCount        = wrapper_estimateCount;
	desc->implementation->estimateSelectivity  = wrapper_estimateSelectivity;
	desc->implementation->searchIndex          = wrapper_searchIndex;
	desc->implementation->getPossiblyExpiredItems = wrapper_getPossiblyExpiredItems;
	desc->implementation->tally                = wrapper_tally;
	desc->implementation->rangeAggregate       = wrapper_rangeAggregate;
	desc->implementation->significant_terms    = wrapper_significant_terms;
	desc->implementation->extended_stats       = wrapper_extended_stats;
	desc->implementation->arbitrary_aggregate  = wrapper_arbitrary_aggregate;
    desc->implementation->suggest_terms        = wrapper_suggest_terms;
	desc->implementation->describeNestedObject = wrapper_describeNestedObject;
	desc->implementation->getIndexMapping      = wrapper_getIndexMapping;
	desc->implementation->highlight			   = wrapper_highlight;
	desc->implementation->freeSearchResponse   = wrapper_freeSearchResponse;
	desc->implementation->bulkDelete           = wrapper_bulkDelete;
	desc->implementation->batchInsertRow       = wrapper_batchInsertRow;
	desc->implementation->batchInsertFinish    = wrapper_batchInsertFinish;
	desc->implementation->commitXactData       = wrapper_commitXactData;
	desc->implementation->transactionFinish    = wrapper_transactionFinish;

	relation_close(heapRel, AccessShareLock);
	MemoryContextSwitchTo(oldContext);

	freeStringInfo(scratch);

	return desc;
}

ZDBIndexDescriptor *zdb_alloc_index_descriptor_by_index_oid(Oid indexrelid)
{
	ZDBIndexDescriptor *desc;
	Relation indexRel;

	indexRel   = relation_open(indexrelid, AccessShareLock);
	desc       = zdb_alloc_index_descriptor(indexRel);
	relation_close(indexRel, AccessShareLock);

	return desc;
}

ZDBCommitXactData *zdb_alloc_new_xact_record(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid)
{
	MemoryContext        oldContext = MemoryContextSwitchTo(TopTransactionContext);
	ZDBCommitNewXactData *xactData;

	xactData = palloc(sizeof(ZDBCommitNewXactData));
	xactData->header.type = ZDB_COMMIT_TYPE_NEW;
	xactData->header.desc = indexDescriptor;
	xactData->header.ctid = palloc(SizeOfIptrData);
	ItemPointerSet(xactData->header.ctid, ItemPointerGetBlockNumber(ctid), ItemPointerGetOffsetNumber(ctid));
	xactData->xmin_is_committed = true;

	MemoryContextSwitchTo(oldContext);

	return (ZDBCommitXactData *) xactData;
}

ZDBCommitXactData *zdb_alloc_expired_xact_record(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, TransactionId xmax, CommandId cmax)
{
	MemoryContext            oldContext = MemoryContextSwitchTo(TopTransactionContext);
	ZDBCommitExpiredXactData *xactData;

	xactData = palloc(sizeof(ZDBCommitExpiredXactData));
	xactData->header.type = ZDB_COMMIT_TYPE_EXPIRED;
	xactData->header.desc = indexDescriptor;
	xactData->header.ctid = palloc(SizeOfIptrData);
	ItemPointerSet(xactData->header.ctid, ItemPointerGetBlockNumber(ctid), ItemPointerGetOffsetNumber(ctid));
	xactData->xmax              = xmax;
	xactData->cmax              = cmax;
	xactData->xmax_is_committed = true;

	MemoryContextSwitchTo(oldContext);

	return (ZDBCommitXactData *) xactData;
}

void zdb_free_index_descriptor(ZDBIndexDescriptor *indexDescriptor)
{
	pfree(indexDescriptor->implementation);
	pfree(indexDescriptor);
}

bool zdb_index_descriptors_equal(ZDBIndexDescriptor *a, ZDBIndexDescriptor *b)
{
	return a == b ||
			(strcmp(a->databaseName, b->databaseName) == 0 &&
					strcmp(a->schemaName, b->schemaName) == 0 &&
					strcmp(a->tableName, b->tableName) == 0 &&
					strcmp(a->indexName, b->indexName) == 0);
}

void zdb_transaction_finish(void)
{

}


char *zdb_multi_search(TransactionId xid, CommandId cid, Oid *indexrelids, char **user_queries, int nqueries) {
	int i;
	ZDBIndexDescriptor **descriptors = (ZDBIndexDescriptor **) palloc(nqueries * (sizeof (ZDBIndexDescriptor*)));

	for (i=0; i<nqueries; i++) {
		descriptors[i] = zdb_alloc_index_descriptor_by_index_oid(indexrelids[i]);

		if (!descriptors[i])
			elog(ERROR, "Unable to create ZDBIndexDescriptor for index oid %d", indexrelids[i]);
	}

	return elasticsearch_multi_search(descriptors, xid, cid, user_queries, nqueries);
}


/** implementation wrapper functions */

static void wrapper_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, bool noxact, char *fieldProperties)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_createNewIndex", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_createNewIndex(indexDescriptor, shards, noxact, fieldProperties);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}

static void wrapper_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_finalizeNewIndex", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_finalizeNewIndex(indexDescriptor);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}

static void wrapper_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_updateMapping", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	if (!indexDescriptor->isShadow)
		elasticsearch_updateMapping(indexDescriptor, mapping);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}

static void wrapper_dropIndex(ZDBIndexDescriptor *indexDescriptor)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_dropIndex", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_dropIndex(indexDescriptor);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}

static void wrapper_refreshIndex(ZDBIndexDescriptor *indexDescriptor)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_refreshIndex", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_refreshIndex(indexDescriptor);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}

static uint64 			  wrapper_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_estimateCount", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);
	uint64        cnt;

	cnt = elasticsearch_actualIndexRecordCount(indexDescriptor, type_name);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
	return cnt;
}


static uint64 wrapper_estimateCount(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_estimateCount", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);
	uint64        cnt;

	cnt = elasticsearch_estimateCount(indexDescriptor, xid, cid, queries, nqueries);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
	return cnt;
}

static uint64 wrapper_estimateSelectivity(ZDBIndexDescriptor *indexDescriptor, char *query)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_estimateSelectivity", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);
	uint64        cnt;

	cnt = elasticsearch_estimateSelectivity(indexDescriptor, query);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
	return cnt;
}

static ZDBSearchResponse *wrapper_searchIndex(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries, uint64 *nhits)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	ZDBSearchResponse *results;

	results = elasticsearch_searchIndex(indexDescriptor, xid, cid, queries, nqueries, nhits);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static ZDBSearchResponse *wrapper_getPossiblyExpiredItems(ZDBIndexDescriptor *indexDescriptor, uint64 *nitems)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	ZDBSearchResponse *results;

	Assert(!indexDescriptor->isShadow);

	results = elasticsearch_getPossiblyExpiredItems(indexDescriptor, nitems);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_tally(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_tally(indexDescriptor, xid, cid, fieldname, stem, query, max_terms, sort_order);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *range_spec, char *query)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_rangeAggregate(indexDescriptor, xid, cid, fieldname, range_spec, query);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_significant_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_significant_terms(indexDescriptor, xid, cid, fieldname, stem, query, max_terms);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_extended_stats(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *user_query)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_extended_stats(indexDescriptor, xid, cid, fieldname, user_query);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *aggregate_query, char *user_query)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_arbitrary_aggregate(indexDescriptor, xid, cid, aggregate_query, user_query);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_suggest_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms)
{
    MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char *results;

    results = elasticsearch_suggest_terms(indexDescriptor, xid, cid, fieldname, stem, query, max_terms);

    MemoryContextSwitchTo(oldContext);
    return results;
}


static char *wrapper_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_describeNestedObject(indexDescriptor, fieldname);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_getIndexMapping(ZDBIndexDescriptor *indexDescriptor)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_getIndexMapping(indexDescriptor);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static char *wrapper_highlight(ZDBIndexDescriptor *indexDescriptor, char *query, char *documentJson)
{
	MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
	char *results;

	results = elasticsearch_highlight(indexDescriptor, query, documentJson);

	MemoryContextSwitchTo(oldContext);
	return results;
}

static void wrapper_freeSearchResponse(ZDBSearchResponse *searchResponse)
{
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	elasticsearch_freeSearchResponse(searchResponse);

	MemoryContextSwitchTo(oldContext);
}

static void wrapper_bulkDelete(ZDBIndexDescriptor *indexDescriptor, List *itemPointers, int nitems)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_bulkDelete", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_bulkDelete(indexDescriptor, itemPointers, nitems);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}

static void wrapper_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, TransactionId xmin, TransactionId xmax, CommandId cmin, CommandId cmax, bool xmin_is_committed, bool xmax_is_committed, text *data)
{
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_batchInsertRow(indexDescriptor, ctid, xmin, xmax, cmin, cmax, xmin_is_committed, xmax_is_committed, data);

	MemoryContextSwitchTo(oldContext);
}

static void wrapper_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor)
{
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_batchInsertFinish(indexDescriptor);

	MemoryContextSwitchTo(oldContext);
}

static void wrapper_commitXactData(ZDBIndexDescriptor *indexDescriptor, List *xactData)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_commitXactData", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	Assert(!indexDescriptor->isShadow);

	elasticsearch_commitXactData(indexDescriptor, xactData);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}

static void wrapper_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType)
{
	MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_transactionFinish", 512, 64, 64);
	MemoryContext oldContext = MemoryContextSwitchTo(me);

	elasticsearch_transactionFinish(indexDescriptor, completionType);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(me);
}
