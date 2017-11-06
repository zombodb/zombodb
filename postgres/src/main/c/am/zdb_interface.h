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
#ifndef ZDB_INTERFACE_H
#define ZDB_INTERFACE_H

#include "postgres.h"

#include "access/reloptions.h"
#include "lib/stringinfo.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

#include "util/zdbutils.h"

#define ZDB_MAX_SHARDS 64
#define ZDB_MAX_REPLICAS 64

/* this needs to match curl_support.h:MAX_CURL_HANDLES */
#define ZDB_MAX_BULK_CONCURRENCY 1024

typedef struct {
    int32 vl_len_;
    /* varlena header (do not touch directly!) */
    int   urlValueOffset;
    int   optionsValueOffset;
    int   shadowValueOffset;
    int   preferenceValueOffset;
    int   refreshIntervalOffset;
    int   shards;
    int   replicas;
    bool  ignoreVisibility;
    int   bulk_concurrency;
    int   batch_size;
    int   fieldListsValueOffset;
    bool  alwaysResolveJoins;
    int   compressionLevel;
    int   aliasOffset;
    int   optimizeAfter;
} ZDBIndexOptions;

#define ZDBIndexOptionsGetUrl(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->urlValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->urlValueOffset : (NULL))

#define ZDBIndexOptionsGetOptions(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->optionsValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->optionsValueOffset : (NULL))

#define ZDBIndexOptionsGetShadow(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->shadowValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->shadowValueOffset : (NULL))

#define ZDBIndexOptionsAlwaysResolveJoins(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->alwaysResolveJoins : false

#define ZDBIndexOptionsGetSearchPreference(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->preferenceValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->preferenceValueOffset : (NULL))

#define ZDBIndexOptionsGetRefreshInterval(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->refreshIntervalOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->refreshIntervalOffset : (NULL))

#define ZDBIndexOptionsGetNumberOfShards(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->shards : 5

#define ZDBIndexOptionsGetNumberOfReplicas(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->replicas : 0

#define ZDBIndexOptionsGetBulkConcurrency(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->bulk_concurrency : 12

#define ZDBIndexOptionsGetBatchSize(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->batch_size : 12

#define ZDBIndexOptionsGetCompressionLevel(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->compressionLevel : 1

#define ZDBIndexOptionsGetFieldLists(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->fieldListsValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->fieldListsValueOffset : (NULL))

#define ZDBIndexOptionsGetIgnoreVisibility(relation) \
    (relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->ignoreVisibility : false

#define ZDBIndexOptionsGetAlias(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->aliasOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->aliasOffset : (NULL))

#define ZDBIndexOptionsGetOptimizeAfter(relation) \
    ((uint64) ((relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->optimizeAfter : 0))


typedef struct ZDBIndexImplementation ZDBIndexImplementation;

typedef struct {
    Oid   indexRelid;
    Oid   heapRelid;
    int64 advisory_mutex;
    bool  isShadow;
    bool  logit;
    bool  alwaysResolveJoins;
    char  *databaseName;
    char  *schemaName;
    char  *tableName;
    char  *indexName;
    char  *fullyQualifiedName;
    char  *pkeyFieldname;
    char  *alias;
    int   shards;
    int   optimizeAfter;
    bool  hasJson;

    char *qualifiedTableName;

    char *url;
    char *options;

    char *searchPreference;
    char *refreshInterval;
    int  bulk_concurrency;
    int  batch_size;
    int  compressionLevel;
    bool ignoreVisibility;

    char *fieldLists;

    ZDBIndexImplementation *implementation;
}                                     ZDBIndexDescriptor;

typedef enum {
    ZDB_TRANSACTION_COMMITTED, ZDB_TRANSACTION_ABORTED
}                                     ZDBTransactionCompletionType;

typedef struct {
    StringInfo httpResponse;
    int64      total_hits;
	char       *hits;  /* don't free directly, should be an offset into httpResponse->data */
    float4     max_score;
}                                     ZDBSearchResponse;

typedef struct {
	ItemPointerData ctid;
	CommandId commandid;
} ZDBDeletedCtidAndCommand;

typedef struct {
    ZDBIndexDescriptor *desc;
	List *deleted;
} ZDBDeletedCtid;

extern PGDLLEXPORT relopt_kind RELOPT_KIND_ZDB;
extern PGDLLEXPORT bool        zdb_batch_mode_guc;
extern PGDLLEXPORT bool        zdb_ignore_visibility_guc;
extern PGDLLEXPORT int         ZDB_LOG_LEVEL;

void               zdb_index_init(void);
void               zdb_transaction_finish(void);
ZDBIndexDescriptor *zdb_alloc_index_descriptor(Relation indexRel);
ZDBIndexDescriptor *zdb_alloc_index_descriptor_by_index_oid(Oid indexrelid);
void               zdb_free_index_descriptor(ZDBIndexDescriptor *indexDescriptor);

char *zdb_multi_search(Oid *indexrelids, char **user_queries, int nqueries);

bool zdb_index_descriptors_equal(ZDBIndexDescriptor *a, ZDBIndexDescriptor *b);
void interface_transaction_cleanup(void);


/**
* Defines what an index implementation looks like
*/
typedef void (*ZDBCreateNewIndex_function)(ZDBIndexDescriptor *indexDescriptor, int shards, char *fieldProperties);
typedef void (*ZDBFinalizeNewIndex_function)(ZDBIndexDescriptor *indexDescriptor);
typedef void (*ZDBUpdateMapping_function)(ZDBIndexDescriptor *indexDescriptor, char *mapping);
typedef char *(*ZDBDumpQuery_function)(ZDBIndexDescriptor *indexDescriptor, char *userQuery);
typedef char *(*ZDBProfileQuery_function)(ZDBIndexDescriptor *indexDescriptor, char *userQuery);

typedef void (*ZDBDropIndex_function)(ZDBIndexDescriptor *indexDescriptor);
typedef void (*ZDBRefreshIndex_function)(ZDBIndexDescriptor *indexDescriptor);

typedef uint64            (*ZDBActualIndexRecordCount_function)(ZDBIndexDescriptor *indexDescriptor, char *table_name);
typedef uint64            (*ZDBEstimateCount_function)(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries);
typedef uint64            (*ZDBEstimateSelectivity_function)(ZDBIndexDescriptor *indexDescriptor, char *query);
typedef ZDBSearchResponse *(*ZDBSearchIndex_function)(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries, uint64 *nhits);
typedef ZDBSearchResponse *(*ZDBGetPossiblyExpiredItems)(ZDBIndexDescriptor *indexDescriptor, uint64 *nitems);

typedef char *(*ZDBTally_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order, int shard_size);
typedef char *(*ZDBRangeAggregate_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *range_spec, char *query);
typedef char *(*ZDBSignificantTerms_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms);
typedef char *(*ZDBExtendedStats_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *user_query);
typedef char *(*ZDBArbitraryAggregate_function)(ZDBIndexDescriptor *indexDescriptor, char *aggregate_query, char *user_query);
typedef char *(*ZDBJsonAggregate_function)(ZDBIndexDescriptor *indexDescriptor, zdb_json json_aggregate, char *user_query);
typedef char *(*ZDBSuggestTerms_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms);
typedef char *(*ZDBTermList_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *prefix, char *startat, uint32 size);

typedef char *(*ZDBDescribeNestedObject_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname);
typedef char *(*ZDBGetIndexMapping_function)(ZDBIndexDescriptor *indexDescriptor);

typedef char *(*ZDBAnalyzeText_function)(ZDBIndexDescriptor *indexDescriptor, char *analyzerName, char *data);

typedef char *(*ZDBHighlight_function)(ZDBIndexDescriptor *indexDescriptor, char *query, zdb_json documentData);

typedef void (*ZDBFreeSearchResponse_function)(ZDBSearchResponse *searchResponse);

typedef void (*ZDBBulkDelete_function)(ZDBIndexDescriptor *indexDescriptor, List *ctidsToDelete);
typedef char *(*ZDBVacuumSupport_function)(ZDBIndexDescriptor *indexDescriptor);
typedef void (*ZDBVacuumCleanup_function)(ZDBIndexDescriptor *indexDescriptor);

typedef void (*ZDBIndexBatchInsertRow_function)(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, text *data, bool isupdate, ItemPointer old_ctid, TransactionId xmin, CommandId commandId, int64 sequence);
typedef void (*ZDBIndexBatchInsertFinish_function)(ZDBIndexDescriptor *indexDescriptor);

typedef void (*ZDBDeleteTuples_function)(ZDBIndexDescriptor *indexDescriptor, List *ctids);

typedef void (*ZDBMarkTransactionCommitted_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid);

typedef void (*ZDBTransactionFinish_function)(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType);

struct ZDBIndexImplementation {
    uint64 _last_selectivity_value;
    char   *_last_selectivity_query;

    ZDBCreateNewIndex_function   createNewIndex;
    ZDBFinalizeNewIndex_function finalizeNewIndex;
    ZDBUpdateMapping_function    updateMapping;
    ZDBDumpQuery_function        dumpQuery;
	ZDBProfileQuery_function     profileQuery;

    ZDBDropIndex_function    dropIndex;

    ZDBActualIndexRecordCount_function actualIndexRecordCount;
    ZDBEstimateCount_function          estimateCount;
    ZDBEstimateSelectivity_function    estimateSelectivity;
    ZDBSearchIndex_function            searchIndex;

    ZDBTally_function              tally;
    ZDBRangeAggregate_function     rangeAggregate;
    ZDBSignificantTerms_function   significant_terms;
    ZDBExtendedStats_function      extended_stats;
    ZDBArbitraryAggregate_function arbitrary_aggregate;
	ZDBJsonAggregate_function      json_aggregate;
    ZDBSuggestTerms_function       suggest_terms;
    ZDBTermList_function           termlist;

    ZDBDescribeNestedObject_function describeNestedObject;
    ZDBGetIndexMapping_function      getIndexMapping;

    ZDBAnalyzeText_function analyzeText;

    ZDBHighlight_function highlight;

    ZDBFreeSearchResponse_function freeSearchResponse;

    ZDBBulkDelete_function bulkDelete;
    ZDBVacuumSupport_function vacuumSupport;
	ZDBVacuumCleanup_function vacuumCleanup;

    ZDBIndexBatchInsertRow_function    batchInsertRow;
    ZDBIndexBatchInsertFinish_function batchInsertFinish;

    ZDBDeleteTuples_function deleteTuples;

	ZDBMarkTransactionCommitted_function markTransactionCommitted;

    ZDBTransactionFinish_function transactionFinish;
};

#endif
