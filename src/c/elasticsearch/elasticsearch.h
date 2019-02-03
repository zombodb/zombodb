/**
 * Copyright 2018-2019 ZomboDB, LLC
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

#ifndef __ZDB_ELSATICSEARCH_H__
#define __ZDB_ELSATICSEARCH_H__

#include "zombodb.h"
#include "json/json_support.h"
#include "rest/curl_support.h"
#include "utils/jsonb.h"

/* this needs to match curl_support.h:MAX_CURL_HANDLES */
#define MAX_BULK_CONCURRENCY 1024

typedef struct ElasticsearchBulkContext {
	char           *url;
	char           *pgIndexName;
	char           *esIndexName;
	char           *typeName;
	int            batchSize;
	int            bulkConcurrency;
	int            compressionLevel;
	bool           waitForActiveShards;
	bool           containsJson;
	bool           containsJsonIsSet;
	bool           shouldRefresh;
	bool           ignoreVersionConflicts;
	MultiRestState *rest;
	PostDataEntry  *current;
	int            nrequests;
	int            nrows;
	int            ntotal;
	int            nindex;
	int            nupdate;
	int            ndelete;
	int            nvacuum;
	int            nxid;
	StringInfo     pool[MAX_CURL_HANDLES];
	TransactionId  lastUsedXid;
	List           *usedXids;    /* should be allocated in TopTransactionContext */
} ElasticsearchBulkContext;

typedef struct ElasticsearchScrollContext {
	MemoryContext jsonMemoryContext;      /* where are json objects allocated? */
	char          *url;
	int           compressionLevel;
	bool          usingId;    /* is this scroll using _id instead of zdb_id? */
	const char    *scrollId;
	bool          hasHighlights;
	uint64        total;      /* total number of hits across all scroll context's */
	uint64        cnt;        /* how many have we examined so far? */
	int           nhits;      /* total number of hits in this scroll context */
	int           currpos;    /* how many have we examined in this scroll context */
	void          *hits;      /* the actual hits in the scroll context, of type 'json_t' */
	void          *hitEntry;
	void          *fields;
	char          **extraFields;
	int           nextraFields;
} ElasticsearchScrollContext;

/* defined in zdbam.c */
extern int ZDB_LOG_LEVEL;

char *make_alias_name(Relation indexRel, bool force_default);

char *ElasticsearchArbitraryRequest(Relation indexRel, char *method, char *endpoint, StringInfo postData);

char *ElasticsearchCreateIndex(Relation heapRel, Relation indexRel, TupleDesc tupdesc, char *aliasName);
void ElasticsearchDeleteIndex(Relation indexRel);
void ElasticsearchDeleteIndexDirect(char *index_url);
void ElasticsearchFinalizeIndexCreation(Relation indexRel);

void ElasticsearchUpdateSettings(Relation indexRel, char *oldAlias, char *newAlias);
void ElasticsearchPutMapping(Relation heapRel, Relation indexRel, TupleDesc tupdesc);

ElasticsearchBulkContext *ElasticsearchStartBulkProcess(Relation indexRel, char *indexName, TupleDesc tupdesc, bool ignore_version_conflicts);
void ElasticsearchBulkInsertRow(ElasticsearchBulkContext *context, ItemPointerData *ctid, text *json, CommandId cmin, CommandId cmax, uint64 xmin, uint64 xmax);
void ElasticsearchBulkUpdateTuple(ElasticsearchBulkContext *context, ItemPointer ctid, char *llapi_id, CommandId cmax, uint64 xmax);
void ElasticsearchBulkVacuumXmax(ElasticsearchBulkContext *context, char *_id, uint64 expected_xmax);
void ElasticsearchBulkDeleteRowByXmin(ElasticsearchBulkContext *context, char *_id, uint64 xmin);
void ElasticsearchBulkDeleteRowByXmax(ElasticsearchBulkContext *context, char *_id, uint64 xmax);
void ElasticsearchFinishBulkProcess(ElasticsearchBulkContext *context, bool is_commit);

uint64 ElasticsearchCountAllDocs(Relation indexRel);
uint64 ElasticsearchEstimateSelectivity(Relation indexRel, ZDBQueryType *query);

ElasticsearchScrollContext *ElasticsearchOpenScroll(Relation indexRel, ZDBQueryType *userQuery, bool use_id, uint64 limit, List *highlights, char **extraFields, int nextraFields);
bool ElasticsearchGetNextItemPointer(ElasticsearchScrollContext *context, ItemPointer ctid, char **_id, float4 *score,
									 zdb_json_object *highlights);
void ElasticsearchCloseScroll(ElasticsearchScrollContext *scrollContext);

void ElasticsearchRemoveAbortedTransactions(Relation indexRel, List/*uint64*/ *xids);

char *ElasticsearchProfileQuery(Relation indexRel, ZDBQueryType *query);

uint64 ElasticsearchCount(Relation indexRel, ZDBQueryType *query);
char *ElasticsearchArbitraryAgg(Relation indexRel, ZDBQueryType *query, char *agg);
char *ElasticsearchTerms(Relation indexRel, char *field, ZDBQueryType *query, char *order, uint64 size);
ArrayType *ElasticsearchTermsAsArray(Relation indexRel, char *field, ZDBQueryType *query, char *order, uint64 size);
char *ElasticsearchTermsTwoLevel(Relation indexRel, char *firstField, char *secondField, ZDBQueryType *query, char *order, uint64 size);
char *ElasticsearchAvg(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchMin(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchMax(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchCardinality(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchSum(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchValueCount(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchPercentiles(Relation indexRel, char *field, ZDBQueryType *query, char *percents);
char *ElasticsearchPercentileRanks(Relation indexRel, char *field, ZDBQueryType *query, char *values);
char *ElasticsearchStats(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchExtendedStats(Relation indexRel, char *field, ZDBQueryType *query, int sigma);
char *ElasticsearchSignificantTerms(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchSignificantTermsTwoLevel(Relation indexRel, char *firstField, char *secondField, ZDBQueryType *query, uint64 size);
char *ElasticsearchRange(Relation indexRel, char *field, ZDBQueryType *query, char *ranges);
char *ElasticsearchDateRange(Relation indexRel, char *field, ZDBQueryType *query, char *ranges);
char *ElasticsearchHistogram(Relation indexRel, char *field, ZDBQueryType *query, float8 interval);
char *ElasticsearchDateHistogram(Relation indexRel, char *field, ZDBQueryType *query, char *interval, char *format);
char *ElasticsearchMissing(Relation indexRel, char *field, ZDBQueryType *query);
char *ElasticsearchFilters(Relation indexRel, char **labels, ZDBQueryType **filters, int nfilters);
char *ElasticsearchIPRange(Relation indexRel, char *field, ZDBQueryType *query, char *ranges);
char *ElasticsearchSignificantText(Relation indexRel, char *field, ZDBQueryType *query, int sample_size, bool filter_duplicate_text);
char *ElasticsearchAdjacencyMatrix(Relation indexRel, char **labels, ZDBQueryType **filters, int nfilters);
char *ElasticsearchMatrixStats(Relation indexRel, ZDBQueryType *query, char **fields, int nfields);
char *ElasticsearchTopHits(Relation indexRel, ZDBQueryType *query, char **fields, int nfields, uint32 size);

char *ElasticsearchSampler(Relation indexRel, uint32 shard_size, ZDBQueryType *query);
char *ElasticsearchDiversifiedSampler(Relation indexRel, uint32 shard_size, char *field, ZDBQueryType *query);
char *ElasticsearchQuerySampler(Relation indexRel, ZDBQueryType *query);

#endif /* __ZDB_ELSATICSEARCH_H__ */
