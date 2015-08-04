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

typedef struct
{
	int32 vl_len_;   /* varlena header (do not touch directly!) */
	int   urlValueOffset;
	int   optionsValueOffset;
	int   shadowValueOffset;
	int   preferenceValueOffset;
	int   shards;
	int   replicas;
	bool  noxact;
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

#define ZDBIndexOptionsGetSearchPreference(relation) \
    ((relation)->rd_options && ((ZDBIndexOptions *) relation->rd_options)->preferenceValueOffset > 0 ? \
      (char *) ((ZDBIndexOptions *) relation->rd_options) + ((ZDBIndexOptions *) relation->rd_options)->preferenceValueOffset : (NULL))

#define ZDBIndexOptionsGetNumberOfShards(relation) \
	(relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->shards : 5

#define ZDBIndexOptionsGetNumberOfReplicas(relation) \
	(relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->replicas : 0

#define ZDBIndexOptionsGetNoXact(relation) \
	(relation)->rd_options ? ((ZDBIndexOptions *) relation->rd_options)->noxact : false

typedef struct ZDBIndexImplementation ZDBIndexImplementation;

typedef struct
{
	Oid  indexRelid;
	int64 advisory_mutex;
	bool isShadow;
	bool logit;
	char *databaseName;
	char *schemaName;
	char *tableName;
	char *indexName;
	char *fullyQualifiedName;

	char *qualifiedTableName;

	char *url;
	char *options;

	char *searchPreference;

	ZDBIndexImplementation *implementation;
}                                     ZDBIndexDescriptor;

typedef enum
{
	ZDB_TRANSACTION_COMMITTED, ZDB_TRANSACTION_ABORTED
}                                     ZDBTransactionCompletionType;
typedef enum
{
	ZDB_COMMIT_TYPE_NEW, ZDB_COMMIT_TYPE_EXPIRED, ZDB_COMMIT_TYPE_ALL
}                                     ZDBCommitXactDataType;

typedef struct
{
	ZDBCommitXactDataType type;
	ZDBIndexDescriptor    *desc;

	ItemPointer ctid;
}                                     ZDBCommitXactData;

typedef struct
{
	ZDBCommitXactData header;

	bool xmin_is_committed;
}                                     ZDBCommitNewXactData;

typedef struct
{
	ZDBCommitXactData header;

	bool          xmax_is_committed;
	TransactionId xmax;
	CommandId     cmax;
}                                     ZDBCommitExpiredXactData;

typedef struct
{
	StringInfo httpResponse;
	int64      total_hits;
	char       *hits; /* don't free directly, should be an offset into httpResponse->data */
}                                     ZDBSearchResponse;

extern PGDLLIMPORT relopt_kind RELOPT_KIND_ZDB;

void               zdb_index_init(void);
void			   zdb_transaction_finish(void);
ZDBIndexDescriptor *zdb_alloc_index_descriptor(Relation indexRel);
ZDBIndexDescriptor *zdb_alloc_index_descriptor_by_index_oid(Oid indexrelid);
void               zdb_free_index_descriptor(ZDBIndexDescriptor *indexDescriptor);
ZDBCommitXactData  *zdb_alloc_new_xact_record(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid);
ZDBCommitXactData  *zdb_alloc_expired_xact_record(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, TransactionId xmax, CommandId cmax);

bool zdb_index_descriptors_equal(ZDBIndexDescriptor *a, ZDBIndexDescriptor *b);


/**
* Defines what an index implementation looks like
*/
typedef void (*ZDBCreateNewIndex_function)(ZDBIndexDescriptor *indexDescriptor, int shards, bool noxact, char *fieldProperties);
typedef void (*ZDBFinalizeNewIndex_function)(ZDBIndexDescriptor *indexDescriptor);
typedef void (*ZDBUpdateMapping_function)(ZDBIndexDescriptor *indexDescriptor, char *mapping);

typedef void (*ZDBDropIndex_function)(ZDBIndexDescriptor *indexDescriptor);
typedef void (*ZDBRefreshIndex_function)(ZDBIndexDescriptor *indexDescriptor);

typedef uint64 (*ZDBActualIndexRecordCount_function)(ZDBIndexDescriptor *indexDescriptor, char *table_name);
typedef uint64 (*ZDBEstimateCount_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries);
typedef ZDBSearchResponse *(*ZDBSearchIndex_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries, uint64 *nhits);
typedef ZDBSearchResponse *(*ZDBGetAllItems_function)(ZDBIndexDescriptor *indexDescriptor, uint64 *nitems);

typedef char *(*ZDBTally_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order);
typedef char *(*ZDBSignificantTerms_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms);
typedef char *(*ZDBExtendedStats_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *user_query);
typedef char *(*ZDBArbitraryAggregate_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *aggregate_query, char *user_query);
typedef char *(*ZDBSuggestTerms_function)(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms);

typedef char *(*ZDBDescribeNestedObject_function)(ZDBIndexDescriptor *indexDescriptor, char *fieldname);
typedef char *(*ZDBGetIndexMapping_function)(ZDBIndexDescriptor *indexDescriptor);

typedef char *(*ZDBHighlight_function)(ZDBIndexDescriptor *indexDescriptor, char *query, char *documentData);

typedef void (*ZDBFreeSearchResponse_function)(ZDBSearchResponse *searchResponse);

typedef void (*ZDBBulkDelete_function)(ZDBIndexDescriptor *indexDescriptor, ItemPointerData *items, int nitems);

typedef void (*ZDBIndexBatchInsertRow_function)(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, TransactionId xmin, TransactionId xmax, CommandId cmin, CommandId cmax, bool xmin_is_committed, bool xmax_is_committed, text *data);
typedef void (*ZDBIndexBatchInsertFinish_function)(ZDBIndexDescriptor *indexDescriptor);
typedef void (*ZDBIndexCommitXactData_function)(ZDBIndexDescriptor *indexDescriptor, List/*<ZDBCommitData *>*/ *datums);

typedef void (*ZDBTransactionFinish_function)(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType);

struct ZDBIndexImplementation
{
	ZDBCreateNewIndex_function   createNewIndex;
	ZDBFinalizeNewIndex_function finalizeNewIndex;
	ZDBUpdateMapping_function    updateMapping;

	ZDBDropIndex_function    dropIndex;
	ZDBRefreshIndex_function refreshIndex;

	ZDBActualIndexRecordCount_function actualIndexRecordCount;
	ZDBEstimateCount_function estimateCount;
	ZDBSearchIndex_function   searchIndex;
	ZDBGetAllItems_function   getAllItems;

	ZDBTally_function              tally;
	ZDBSignificantTerms_function   significant_terms;
	ZDBExtendedStats_function      extended_stats;
	ZDBArbitraryAggregate_function arbitrary_aggregate;
    ZDBSuggestTerms_function       suggest_terms;

	ZDBDescribeNestedObject_function describeNestedObject;
	ZDBGetIndexMapping_function getIndexMapping;

	ZDBHighlight_function highlight;

	ZDBFreeSearchResponse_function freeSearchResponse;

	ZDBBulkDelete_function bulkDelete;

	ZDBIndexBatchInsertRow_function    batchInsertRow;
	ZDBIndexBatchInsertFinish_function batchInsertFinish;

	ZDBIndexCommitXactData_function commitXactData;

	ZDBTransactionFinish_function transactionFinish;
};

#endif
