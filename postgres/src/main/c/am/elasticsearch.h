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

void elasticsearch_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, bool noxact, char *fieldProperties);
void elasticsearch_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor);
void elasticsearch_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping);

void elasticsearch_dropIndex(ZDBIndexDescriptor *indexDescriptor);
void elasticsearch_refreshIndex(ZDBIndexDescriptor *indexDescriptor);

uint64             elasticsearch_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name);
uint64             elasticsearch_estimateCount(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries);
ZDBSearchResponse *elasticsearch_searchIndex(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries, uint64 *nhits);
ZDBSearchResponse *elasticsearch_getPossiblyExpiredItems(ZDBIndexDescriptor *indexDescriptor, uint64 *nitems);

char *elasticsearch_tally(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms, char *sort_order);
char *elasticsearch_significant_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms);
char *elasticsearch_extended_stats(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *user_query);
char *elasticsearch_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *aggregate_query, char *user_query);
char *elasticsearch_suggest_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *query, int64 max_terms);

char *elasticsearch_getIndexMapping(ZDBIndexDescriptor *indexDescriptor);
char *elasticsearch_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname);

char *elasticsearch_highlight(ZDBIndexDescriptor *indexDescriptor, char *query, char *documentJson);

void elasticsearch_freeSearchResponse(ZDBSearchResponse *searchResponse);

void elasticsearch_bulkDelete(ZDBIndexDescriptor *indexDescriptor, List *itemPointers, int nitems);

void elasticsearch_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, TransactionId xmin, TransactionId xmax, CommandId cmin, CommandId cmax, bool xmin_is_committed, bool xmax_is_committed, text *data);
void elasticsearch_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor);

void elasticsearch_commitXactData(ZDBIndexDescriptor *indexDescriptor, List *xactData);

void elasticsearch_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType);

#endif
