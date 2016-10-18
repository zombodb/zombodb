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
#ifndef ELASTICSEARCH_H
#define ELASTICSEARCH_H

#include "zdb_interface.h"

void elasticsearch_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, char *fieldProperties);
void elasticsearch_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor, HTAB *committedXids);
void elasticsearch_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping);
char *elasticsearch_dumpQuery(ZDBIndexDescriptor *indexDescriptor, char *userQuery);

void elasticsearch_dropIndex(ZDBIndexDescriptor *indexDescriptor);
void elasticsearch_refreshIndex(ZDBIndexDescriptor *indexDescriptor);

char *elasticsearch_multi_search(ZDBIndexDescriptor **descriptors, char **user_queries, int nqueries);


uint64            elasticsearch_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name);
uint64            elasticsearch_estimateCount(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries);
uint64            elasticsearch_estimateSelectivity(ZDBIndexDescriptor *indexDescriptor, char *query);
ZDBSearchResponse *elasticsearch_searchIndex(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries, uint64 *nhits);

char *elasticsearch_tally(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order, int shard_size);
char *elasticsearch_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *range_spec, char *query);
char *elasticsearch_significant_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms);
char *elasticsearch_extended_stats(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *user_query);
char *elasticsearch_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, char *aggregate_query, char *user_query);
char *elasticsearch_json_aggregate(ZDBIndexDescriptor *indexDescriptor, zdb_json json_agg, char *user_query);
char *elasticsearch_suggest_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *query, int64 max_terms);
char *elasticsearch_termlist(ZDBIndexDescriptor *descriptor, char *fieldname, char *prefix, char *startat, uint32 size);


char *elasticsearch_getIndexMapping(ZDBIndexDescriptor *indexDescriptor);
char *elasticsearch_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname);

char *elasticsearch_analyzeText(ZDBIndexDescriptor *indexDescriptor, char *analyzerName, char *data);

char *elasticsearch_highlight(ZDBIndexDescriptor *indexDescriptor, char *query, zdb_json documentJson);

void elasticsearch_freeSearchResponse(ZDBSearchResponse *searchResponse);

void elasticsearch_bulkDelete(ZDBIndexDescriptor *indexDescriptor, ItemPointer itemPointers, int nitems);

void elasticsearch_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, text *data, bool isupdate, ItemPointer old_ctid, TransactionId xid, CommandId commandId, uint64 sequence);
void elasticsearch_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor);

void elasticsearch_markTransactionCommitted(ZDBIndexDescriptor *indexDescriptor, TransactionId xid);

void elasticsearch_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType);

#endif
