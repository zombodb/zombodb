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
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "zdb_interface.h"
#include "elasticsearch.h"

#define XACT_INDEX_SUFFIX "_zdb_xact"

relopt_kind RELOPT_KIND_ZDB;
bool        zdb_batch_mode_guc;
bool        zdb_ignore_visibility_guc;

static void wrapper_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, char *fieldProperties);
static void wrapper_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor);
static void wrapper_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping);
static char *wrapper_dumpQuery(ZDBIndexDescriptor *indexDescriptor, char *userQuery);

static void wrapper_dropIndex(ZDBIndexDescriptor *indexDescriptor);

static uint64            wrapper_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name);
static uint64            wrapper_estimateCount(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries);
static uint64            wrapper_estimateSelectivity(ZDBIndexDescriptor *indexDescriptor, char *query);
static ZDBSearchResponse *wrapper_searchIndex(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries, uint64 *nhits);

static char *wrapper_tally(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order, int shard_size);
static char *wrapper_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *range_spec, char *query);
static char *wrapper_significant_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms);
static char *wrapper_extended_stats(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *user_query);
static char *wrapper_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, char *aggregate_query, char *user_query);
static char *wrapper_suggest_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms);
static char *wrapper_termlist(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *prefix, char *startat, uint32 size);

static char *wrapper_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname);
static char *wrapper_getIndexMapping(ZDBIndexDescriptor *indexDescriptor);

static char *wrapper_analyzeText(ZDBIndexDescriptor *indexDescriptor, char *analyzerName, char *data);

static char *wrapper_highlight(ZDBIndexDescriptor *indexDescriptor, char *query, zdb_json documentJson);

static void wrapper_freeSearchResponse(ZDBSearchResponse *searchResponse);

static void wrapper_bulkDelete(ZDBIndexDescriptor *indexDescriptor, ItemPointer itemPointers, int nitems);

static void wrapper_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, text *data, bool isupdate, uint64 pkey, ItemPointer old_ctid, TransactionId xmin);
static void wrapper_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor);

static uint64 *wrapper_vacuumSupport(ZDBIndexDescriptor *indexDescriptor, zdb_json jsonXids, uint32 *nxids);

static void wrapper_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType);

static void validate_url(char *str) {
    if (str && str[strlen(str) - 1] != '/')
        elog(ERROR, "'url' index option must end in a slash");
}

static void validate_shadow(char *str) {
    if (str && DatumGetObjectId(DirectFunctionCall1(regclassin, PointerGetDatum(str))) == InvalidOid)
        elog(ERROR, "Invalid shadow index name: %s", str);
}

static void validate_options(char *str) {
    // TODO:  implement this
}

static void validate_preference(char *str) {
    // noop
}

static void validate_field_lists(char *str) {
    // TODO:  implement this
}

static void validate_refresh_interval(char *str) {
    // noop
}

static List *allocated_descriptors = NULL;

void zdb_index_init(void) {
    RELOPT_KIND_ZDB = add_reloption_kind();

    add_string_reloption(RELOPT_KIND_ZDB, "url", "Server URL and port", NULL, validate_url);
    add_string_reloption(RELOPT_KIND_ZDB, "shadow", "A zombodb index to which this one should shadow", NULL, validate_shadow);
    add_string_reloption(RELOPT_KIND_ZDB, "options", "Comma-separated list of options to pass to underlying index", NULL, validate_options);
    add_string_reloption(RELOPT_KIND_ZDB, "preference", "The ?preference value used to Elasticsearch", NULL, validate_preference);
    add_string_reloption(RELOPT_KIND_ZDB, "refresh_interval", "Frequency in which Elasticsearch indexes are refreshed.  Related to ES' index.refresh_interval setting", "-1", validate_refresh_interval);
    add_int_reloption(RELOPT_KIND_ZDB, "shards", "The number of shared for the index", 5, 1, ZDB_MAX_SHARDS);
    add_int_reloption(RELOPT_KIND_ZDB, "replicas", "The default number of replicas for the index", 1, 0, ZDB_MAX_REPLICAS);
    add_int_reloption(RELOPT_KIND_ZDB, "bulk_concurrency", "The maximum number of concurrent _bulk API requests", 12, 1, ZDB_MAX_BULK_CONCURRENCY);
	add_int_reloption(RELOPT_KIND_ZDB, "batch_size", "The size in bytes of batch calls to the _bulk API", 1024 * 1024 * 8, 1024, (INT32_MAX/2)-1);
    add_string_reloption(RELOPT_KIND_ZDB, "field_lists", "field=[field1, field2, field3], other=[field4,field5]", NULL, validate_field_lists);
    add_bool_reloption(RELOPT_KIND_ZDB, "ignore_visibility", "Should queries that require visibility information actually use it?", false);
    add_bool_reloption(RELOPT_KIND_ZDB, "always_resolve_joins", "Should queries that link to other indexes always resolve the links", false);
    add_int_reloption(RELOPT_KIND_ZDB, "compression_level", "0-9 value to indicate the level of HTTP compression", 0, 0, 9);

    DefineCustomBoolVariable("zombodb.batch_mode", "Batch INSERT/UPDATE/COPY changes until transaction commit", NULL, &zdb_batch_mode_guc, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("zombodb.ignore_visibility", "If true, visibility information will be ignored for all queries", NULL, &zdb_ignore_visibility_guc, false, PGC_USERSET, 0, NULL, NULL, NULL);
}

ZDBIndexDescriptor *zdb_alloc_index_descriptor(Relation indexRel) {
    MemoryContext      oldContext = MemoryContextSwitchTo(TopTransactionContext);
    StringInfo         scratch    = makeStringInfo();
    TupleDesc          heapTupDesc;
    Relation           heapRel;
    ZDBIndexDescriptor *desc;
    ListCell           *lc;
    int                i;

    if (indexRel->rd_index == NULL)
        elog(ERROR, "%s is not an index", RelationGetRelationName(indexRel));

    foreach(lc, allocated_descriptors) {
        desc = lfirst(lc);
        if (desc->indexRelid == indexRel->rd_id) {
            return desc;
        }
    }

    heapRel = relation_open(indexRel->rd_index->indrelid, AccessShareLock);

    desc = palloc0(sizeof(ZDBIndexDescriptor));

    /* these all come from the actual index */
    desc->indexRelid   = RelationGetRelid(indexRel);
    desc->heapRelid    = RelationGetRelid(heapRel);
    desc->isShadow     = ZDBIndexOptionsGetShadow(indexRel) != NULL;
    desc->logit        = false;
    desc->databaseName = pstrdup(get_database_name(MyDatabaseId));
    desc->schemaName   = pstrdup(get_namespace_name(RelationGetNamespace(heapRel)));
    desc->tableName    = pstrdup(RelationGetRelationName(heapRel));
	desc->options	   = ZDBIndexOptionsGetOptions(indexRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetOptions(indexRel));

    desc->pkeyFieldname = pstrdup(lookup_primary_key(desc->schemaName, desc->tableName));

    desc->searchPreference   = ZDBIndexOptionsGetSearchPreference(indexRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetSearchPreference(indexRel));
    desc->refreshInterval    = ZDBIndexOptionsGetRefreshInterval(indexRel) ? pstrdup("-1") : pstrdup(ZDBIndexOptionsGetRefreshInterval(indexRel));
    desc->bulk_concurrency   = ZDBIndexOptionsGetBulkConcurrency(indexRel);
    desc->batch_size         = ZDBIndexOptionsGetBatchSize(indexRel);
    desc->ignoreVisibility   = ZDBIndexOptionsGetIgnoreVisibility(indexRel);
    desc->fieldLists         = ZDBIndexOptionsGetFieldLists(indexRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetFieldLists(indexRel));
    desc->alwaysResolveJoins = ZDBIndexOptionsAlwaysResolveJoins(indexRel);

    heapTupDesc = RelationGetDescr(heapRel);
    for (i = 0; i < heapTupDesc->natts; i++) {
        if (heapTupDesc->attrs[i]->atttypid == JSONOID) {
            desc->hasJson = true;
            break;
        }
    }

    if (desc->isShadow) {
        /* but some properties come from the index we're shadowing */
        Oid      shadowRelid = DatumGetObjectId(DirectFunctionCall1(regclassin, PointerGetDatum(ZDBIndexOptionsGetShadow(indexRel))));
        Relation shadowRel;

        if (shadowRelid == InvalidOid)
            elog(ERROR, "No such shadow index: %s", ZDBIndexOptionsGetShadow(indexRel));

        shadowRel = relation_open(shadowRelid, AccessShareLock);
        desc->shards    = ZDBIndexOptionsGetNumberOfShards(shadowRel);
        desc->indexName = pstrdup(RelationGetRelationName(shadowRel));
        desc->url       = ZDBIndexOptionsGetUrl(shadowRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetUrl(shadowRel));

        desc->compressionLevel  = ZDBIndexOptionsGetCompressionLevel(shadowRel);

        relation_close(shadowRel, AccessShareLock);
    } else {
        /* or just from the actual index if we're not a shadow */
        desc->shards    = ZDBIndexOptionsGetNumberOfShards(indexRel);
        desc->indexName = pstrdup(RelationGetRelationName(indexRel));
        desc->url       = ZDBIndexOptionsGetUrl(indexRel) == NULL ? NULL : pstrdup(ZDBIndexOptionsGetUrl(indexRel));

        desc->compressionLevel = ZDBIndexOptionsGetCompressionLevel(indexRel);
    }

    desc->advisory_mutex = (int64) string_hash(desc->indexName, strlen(desc->indexName));

    appendStringInfo(scratch, "%s.%s.%s.%s", desc->databaseName, desc->schemaName, desc->tableName, desc->indexName);
    desc->fullyQualifiedName = pstrdup(scratch->data);

    resetStringInfo(scratch);
    appendStringInfo(scratch, "%s.%s", get_namespace_name(RelationGetNamespace(heapRel)), RelationGetRelationName(heapRel));
    desc->qualifiedTableName = pstrdup(scratch->data);

    desc->implementation                          = palloc0(sizeof(ZDBIndexImplementation));
    desc->implementation->_last_selectivity_query = NULL;

    desc->implementation->createNewIndex          = wrapper_createNewIndex;
    desc->implementation->finalizeNewIndex        = wrapper_finalizeNewIndex;
    desc->implementation->updateMapping           = wrapper_updateMapping;
    desc->implementation->dumpQuery               = wrapper_dumpQuery;
    desc->implementation->dropIndex               = wrapper_dropIndex;
    desc->implementation->actualIndexRecordCount  = wrapper_actualIndexRecordCount;
    desc->implementation->estimateCount           = wrapper_estimateCount;
    desc->implementation->estimateSelectivity     = wrapper_estimateSelectivity;
    desc->implementation->searchIndex             = wrapper_searchIndex;
    desc->implementation->tally                   = wrapper_tally;
    desc->implementation->rangeAggregate          = wrapper_rangeAggregate;
    desc->implementation->significant_terms       = wrapper_significant_terms;
    desc->implementation->extended_stats          = wrapper_extended_stats;
    desc->implementation->arbitrary_aggregate     = wrapper_arbitrary_aggregate;
    desc->implementation->suggest_terms           = wrapper_suggest_terms;
    desc->implementation->termlist                = wrapper_termlist;
    desc->implementation->describeNestedObject    = wrapper_describeNestedObject;
    desc->implementation->getIndexMapping         = wrapper_getIndexMapping;
    desc->implementation->analyzeText             = wrapper_analyzeText;
    desc->implementation->highlight               = wrapper_highlight;
    desc->implementation->freeSearchResponse      = wrapper_freeSearchResponse;
    desc->implementation->bulkDelete              = wrapper_bulkDelete;
    desc->implementation->batchInsertRow          = wrapper_batchInsertRow;
    desc->implementation->batchInsertFinish       = wrapper_batchInsertFinish;
    desc->implementation->vacuumSupport           = wrapper_vacuumSupport;
    desc->implementation->transactionFinish       = wrapper_transactionFinish;

    allocated_descriptors = lappend(allocated_descriptors, desc);

    relation_close(heapRel, AccessShareLock);
    MemoryContextSwitchTo(oldContext);

    freeStringInfo(scratch);

    return desc;
}

ZDBIndexDescriptor *zdb_alloc_index_descriptor_by_index_oid(Oid indexrelid) {
    ZDBIndexDescriptor *desc;
    Relation           indexRel;

    indexRel = relation_open(indexrelid, AccessShareLock);
    desc     = zdb_alloc_index_descriptor(indexRel);
    relation_close(indexRel, AccessShareLock);

    return desc;
}

void zdb_free_index_descriptor(ZDBIndexDescriptor *indexDescriptor) {
    pfree(indexDescriptor->implementation);
    pfree(indexDescriptor);
}

bool zdb_index_descriptors_equal(ZDBIndexDescriptor *a, ZDBIndexDescriptor *b) {
    return a == b || (strcmp(a->databaseName, b->databaseName) == 0 && strcmp(a->schemaName, b->schemaName) == 0 &&
                      strcmp(a->tableName, b->tableName) == 0 && strcmp(a->indexName, b->indexName) == 0);
}

void zdb_transaction_finish(void) {

}


char *zdb_multi_search(Oid *indexrelids, char **user_queries, int nqueries) {
    int                i;
    ZDBIndexDescriptor **descriptors = (ZDBIndexDescriptor **) palloc(nqueries * (sizeof(ZDBIndexDescriptor *)));

    for (i = 0; i < nqueries; i++) {
        descriptors[i] = zdb_alloc_index_descriptor_by_index_oid(indexrelids[i]);

        if (!descriptors[i])
            elog(ERROR, "Unable to create ZDBIndexDescriptor for index oid %d", indexrelids[i]);
    }

    return elasticsearch_multi_search(descriptors, user_queries, nqueries);
}


/** implementation wrapper functions */

static void wrapper_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, char *fieldProperties) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_createNewIndex", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);

    Assert(!indexDescriptor->isShadow);

    elasticsearch_createNewIndex(indexDescriptor, shards, fieldProperties);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
}

static void wrapper_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_finalizeNewIndex", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);

    Assert(!indexDescriptor->isShadow);

    elasticsearch_finalizeNewIndex(indexDescriptor);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
}

static void wrapper_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_updateMapping", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);

    if (!indexDescriptor->isShadow)
        elasticsearch_updateMapping(indexDescriptor, mapping);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
}

static char *wrapper_dumpQuery(ZDBIndexDescriptor *indexDescriptor, char *userQuery) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *jsonQuery;

    jsonQuery = elasticsearch_dumpQuery(indexDescriptor, userQuery);

    MemoryContextSwitchTo(oldContext);
    return jsonQuery;
}

static void wrapper_dropIndex(ZDBIndexDescriptor *indexDescriptor) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_dropIndex", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);

    Assert(!indexDescriptor->isShadow);

    elasticsearch_dropIndex(indexDescriptor);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
}

static uint64 wrapper_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_estimateCount", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);
    uint64        cnt;

    cnt = elasticsearch_actualIndexRecordCount(indexDescriptor, type_name);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
    return cnt;
}


static uint64 wrapper_estimateCount(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_estimateCount", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);
    uint64        cnt;

    cnt = elasticsearch_estimateCount(indexDescriptor, queries, nqueries);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
    return cnt;
}

static uint64 wrapper_estimateSelectivity(ZDBIndexDescriptor *indexDescriptor, char *query) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_estimateSelectivity", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);
    uint64        cnt;

    if (indexDescriptor->implementation->_last_selectivity_query == NULL ||
        strcmp(query, indexDescriptor->implementation->_last_selectivity_query) != 0) {
        cnt = elasticsearch_estimateSelectivity(indexDescriptor, query);

        /* remember this query/count for next time */
        indexDescriptor->implementation->_last_selectivity_value = cnt;
        indexDescriptor->implementation->_last_selectivity_query = MemoryContextAlloc(TopTransactionContext,
                                                                                      strlen(query) + 1);
        strcpy(indexDescriptor->implementation->_last_selectivity_query, query);
    } else {
        cnt = indexDescriptor->implementation->_last_selectivity_value;
    }

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
    return cnt;
}

static ZDBSearchResponse *wrapper_searchIndex(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries, uint64 *nhits) {
    MemoryContext     oldContext = MemoryContextSwitchTo(TopTransactionContext);
    ZDBSearchResponse *results;

    results = elasticsearch_searchIndex(indexDescriptor, queries, nqueries, nhits);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_tally(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order, int shard_size) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_tally(indexDescriptor, fieldname, stem, query, max_terms, sort_order, shard_size);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *range_spec, char *query) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_rangeAggregate(indexDescriptor, fieldname, range_spec, query);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_significant_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_significant_terms(indexDescriptor, fieldname, stem, query, max_terms);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_extended_stats(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *user_query) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_extended_stats(indexDescriptor, fieldname, user_query);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, char *aggregate_query, char *user_query) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_arbitrary_aggregate(indexDescriptor, aggregate_query, user_query);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_suggest_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_suggest_terms(indexDescriptor, fieldname, stem, query, max_terms);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_termlist(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *prefix, char *startat, uint32 size) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_termlist(indexDescriptor, fieldname, prefix, startat, size);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_describeNestedObject(indexDescriptor, fieldname);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_getIndexMapping(ZDBIndexDescriptor *indexDescriptor) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_getIndexMapping(indexDescriptor);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_analyzeText(ZDBIndexDescriptor *indexDescriptor, char *analyzerName, char *data) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_analyzeText(indexDescriptor, analyzerName, data);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static char *wrapper_highlight(ZDBIndexDescriptor *indexDescriptor, char *query, zdb_json documentJson) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    char          *results;

    results = elasticsearch_highlight(indexDescriptor, query, documentJson);

    MemoryContextSwitchTo(oldContext);
    return results;
}

static void wrapper_freeSearchResponse(ZDBSearchResponse *searchResponse) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

    elasticsearch_freeSearchResponse(searchResponse);

    MemoryContextSwitchTo(oldContext);
}

static void wrapper_bulkDelete(ZDBIndexDescriptor *indexDescriptor, ItemPointer itemPointers, int nitems) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_bulkDelete", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);

    Assert(!indexDescriptor->isShadow);

    elasticsearch_bulkDelete(indexDescriptor, itemPointers, nitems);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
}

static void wrapper_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, text *data, bool isupdate, uint64 pkey, ItemPointer old_ctid, TransactionId xmin) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

    Assert(!indexDescriptor->isShadow);

    elasticsearch_batchInsertRow(indexDescriptor, ctid, data, isupdate, pkey, old_ctid, xmin);

    MemoryContextSwitchTo(oldContext);
}

static void wrapper_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

    Assert(!indexDescriptor->isShadow);

    elasticsearch_batchInsertFinish(indexDescriptor);

    MemoryContextSwitchTo(oldContext);
}

static uint64 *wrapper_vacuumSupport(ZDBIndexDescriptor *indexDescriptor, zdb_json jsonXids, uint32 *nxids) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    uint64 *xids;

    Assert(!indexDescriptor->isShadow);

    xids = elasticsearch_vacuumSupport(indexDescriptor, jsonXids, nxids);

    MemoryContextSwitchTo(oldContext);

    return xids;
}

static void wrapper_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType) {
    MemoryContext me         = AllocSetContextCreate(TopTransactionContext, "wrapper_transactionFinish", 512, 64, 64);
    MemoryContext oldContext = MemoryContextSwitchTo(me);

    elasticsearch_transactionFinish(indexDescriptor, completionType);

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(me);
}

void interface_transaction_cleanup(void) {
    allocated_descriptors = NULL;
}
