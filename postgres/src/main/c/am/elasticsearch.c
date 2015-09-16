/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015 ZomboDB, LLC
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
#include "fmgr.h"
#include "miscadmin.h"
#include "access/genam.h"
#include "access/xact.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/json.h"

#include "rest/rest.h"
#include "util/zdbutils.h"

#include "elasticsearch.h"
#include "zdb_interface.h"


typedef struct
{
	ZDBIndexDescriptor *indexDescriptor;
	MultiRestState     *rest;
	StringInfo         bulk;
	int                nprocessed;
	int				   nrequests;
} BatchInsertData;

static List *batchInsertDataList = NULL;

static BatchInsertData *lookup_batch_insert_data(ZDBIndexDescriptor *indexDescriptor, bool create)
{
	BatchInsertData *data = NULL;
	ListCell        *lc;

	foreach(lc, batchInsertDataList)
	{
		BatchInsertData *tmp = lfirst(lc);

		if (tmp->indexDescriptor->indexRelid == indexDescriptor->indexRelid)
		{
			data = tmp;
			break;
		}
	}

	if (!data && create)
	{
		data = palloc(sizeof(BatchInsertData));
		data->indexDescriptor = indexDescriptor;
		data->rest			  = palloc0(sizeof(MultiRestState));
		data->bulk            = makeStringInfo();
		data->nprocessed      = 0;
		data->nrequests		  = 0;
		batchInsertDataList = lappend(batchInsertDataList, data);

		rest_multi_init(data->rest, indexDescriptor->bulk_concurrency);
	}

	return data;
}

static StringInfo buildQuery(ZDBIndexDescriptor *desc, TransactionId xid, CommandId cid, char **queries, int nqueries, bool isParentQuery)
{
	StringInfo baseQuery = makeStringInfo();
	int i;

	if (desc->options)
		appendStringInfo(baseQuery, "#options(%s) ", desc->options);

	if (isParentQuery)
		appendStringInfo(baseQuery, "#parent<xact>(");
	appendStringInfoCharMacro(baseQuery, '(');
	/* ... from HeapTupleSatisfiesNow() */
	appendStringInfo(baseQuery,
			"((_xmin = %d AND "               /* inserted by the current transaction */
			"_cmin < %d AND "                 /* before this command, and */
			"(_xmax = 0 OR "                  /* the row has not been deleted, or */
			"(_xmax = %d AND "                /* it was deleted by the current transaction */
			"_cmax >= %d))) "                 /* but not before this command, */
			"OR "                             /* or */
			"(_xmin_is_committed = true AND " /* the row was inserted by a committed transaction, and */
			"(_xmax = 0 OR "                  /* the row has not been deleted, or */
			"(_xmax = %d AND "                /* the row is being deleted by this transaction */
			"_cmax >= %d) OR "                /* but it's not deleted \"yet\", or */
			"(_xmax <> %d AND "               /* the row was deleted by another transaction */
			"_xmax_is_committed = false))))", /* that has not been committed */
			xid, cid, xid, cid,
			xid, cid, xid
	);
	appendStringInfoCharMacro(baseQuery, ')');
	if (isParentQuery)
		appendStringInfoCharMacro(baseQuery, ')');


	appendStringInfo(baseQuery, " AND (");
	if (!isParentQuery)
		appendStringInfo(baseQuery, "#child<data>(");
	for (i = 0; i < nqueries; i++)
	{
		if (i > 0) appendStringInfo(baseQuery, " AND ");
		appendStringInfo(baseQuery, "(%s)", queries[i]);
	}
	appendStringInfoCharMacro(baseQuery, ')');	/* close off AND */
	if (!isParentQuery)
		appendStringInfoCharMacro(baseQuery, ')');  /* close off #child<data> */

	return baseQuery;
}

static void checkForBulkError(StringInfo response, char *type) {
	text *errorsText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("errors")));
	if (errorsText == NULL)
		elog(IsTransactionState() ? ERROR : WARNING, "Unexpected response from elasticsearch during %s: %s", type, response->data);
	else
	{
		char *errors = TextDatumGetCString(errorsText);
		if (strcmp(errors, "false") != 0)
			elog(IsTransactionState() ? ERROR : WARNING, "Error updating %s data: %s", type, response->data);
		pfree(errors);
		pfree(errorsText);
	}
}

static void checkForRefreshError(StringInfo response) {
	Datum shards = DirectFunctionCall2(json_object_field, CStringGetTextDatum(response->data), CStringGetTextDatum("_shards"));
	text *failedText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, shards, CStringGetTextDatum("failed")));
	if (failedText == NULL)
		elog(IsTransactionState() ? ERROR : WARNING, "Unexpected response from elasticsearch during _refresh: %s", response->data);
	else
	{
		char *errors = TextDatumGetCString(failedText);
		if (strcmp(errors, "0") != 0)
			elog(IsTransactionState() ? ERROR : WARNING, "Error refresing: %s", response->data);
		pfree(errors);
		pfree(failedText);
	}
}

void elasticsearch_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, bool noxact, char *fieldProperties)
{
	StringInfo endpoint      = makeStringInfo();
	StringInfo indexSettings = makeStringInfo();
	StringInfo response;
	char *pkey = lookup_primary_key(indexDescriptor->schemaName, indexDescriptor->tableName, false);

	if (pkey == NULL)
	{
		pkey = "__no primary key__";
		elog(WARNING, "No primary key detected for %s.%s, continuing anyway", indexDescriptor->schemaName, indexDescriptor->tableName);
	}

	appendStringInfo(indexSettings,
			"{"
					"   \"mappings\": {"
					"      \"xact\": {"
					"          \"_source\": { \"enabled\": true },"
					"          \"_all\": { \"enabled\": false },"
					"          \"_field_names\": { \"index\": \"no\", \"store\": false },"
					"          \"_meta\": { \"primary_key\": \"%s\", \"noxact\": %s },"
					"          \"date_detection\": false,"
					"          \"properties\" : {"
					"              \"_xmin\": { \"type\": \"integer\", \"index\": \"not_analyzed\", \"norms\": {\"enabled\":false}, \"fielddata\": {\"format\": \"disabled\"} },"
					"              \"_xmax\": { \"type\": \"integer\", \"index\": \"not_analyzed\", \"norms\": {\"enabled\":false}, \"fielddata\": {\"format\": \"disabled\"} },"
					"              \"_cmin\": { \"type\": \"integer\", \"index\": \"not_analyzed\", \"norms\": {\"enabled\":false}, \"fielddata\": {\"format\": \"disabled\"} },"
					"              \"_cmax\": { \"type\": \"integer\", \"index\": \"not_analyzed\", \"norms\": {\"enabled\":false}, \"fielddata\": {\"format\": \"disabled\"} },"
					"              \"_xmin_is_committed\": { \"type\": \"boolean\", \"index\": \"not_analyzed\", \"norms\": {\"enabled\":false}, \"fielddata\": {\"format\": \"disabled\"} },"
					"              \"_xmax_is_committed\": { \"type\": \"boolean\", \"index\": \"not_analyzed\", \"norms\": {\"enabled\":false}, \"fielddata\": {\"format\": \"disabled\"} },"
					"              \"_partial\": { \"type\": \"boolean\", \"index\": \"not_analyzed\", \"norms\": {\"enabled\":false}, \"fielddata\": {\"format\": \"disabled\"} }"
					"          }"
					"      },"
					"      \"data\": {"
					"          \"_source\": { \"enabled\": false },"
					"          \"_all\": { \"enabled\": true, \"analyzer\": \"phrase\" },"
					"          \"_field_names\": { \"index\": \"no\", \"store\": false },"
					"          \"_parent\": { \"type\": \"xact\" },"
					"          \"_meta\": { \"primary_key\": \"%s\", \"noxact\": %s },"
					"          \"date_detection\": false,"
					"          \"properties\" : %s"
					"      }"
					"   },"
					"   \"settings\": {"
					"      \"refresh_interval\": -1,"
					"      \"number_of_shards\": %d,"
					"      \"number_of_replicas\": 0,"
					"      \"analysis\": {"
					"         \"filter\": {"
					"             \"truncate_32000\": { \"type\": \"truncate\", \"length\": 32000 }"
					"         },"
					"         \"analyzer\": {"
					"            \"default\": {"
					"               \"tokenizer\": \"keyword\","
					"               \"filter\": [\"trim\", \"lowercase\", \"truncate_32000\"]"
					"            },"
					"            \"exact\": {"
					"               \"tokenizer\": \"keyword\","
					"               \"filter\": [\"trim\", \"lowercase\", \"truncate_32000\"]"
					"            },"
					"            \"phrase\": {"
					"               \"tokenizer\": \"standard\","
					"               \"filter\": [\"lowercase\"]"
					"            },"
					"            \"fulltext\": {"
					"               \"tokenizer\": \"standard\","
					"               \"filter\": [\"lowercase\"]"
					"            }"
					"         }"
					"      }"
					"   }"
					"}",
			pkey, noxact ? "true" : "false",
			pkey, noxact ? "true" : "false",
			fieldProperties,
			shards
	);

	appendStringInfo(endpoint, "%s/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("POST", endpoint->data, indexSettings);

	freeStringInfo(indexSettings);
	freeStringInfo(response);
	freeStringInfo(endpoint);
}

void elasticsearch_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor)
{
	StringInfo endpoint      = makeStringInfo();
	StringInfo indexSettings = makeStringInfo();
	StringInfo response;
	Relation indexRel;

	indexRel = index_open(indexDescriptor->indexRelid, RowExclusiveLock);
	appendStringInfo(indexSettings,
			"{"
					"   \"index\": {"
					    /* NOTE:  We DO NOT turn refresh_interval back on -- we control refreshes ourselves */
					"      \"number_of_replicas\":%d"
					"   }"
					"}",
			ZDBIndexOptionsGetNumberOfReplicas(indexRel)
	);
	index_close(indexRel, RowExclusiveLock);

	appendStringInfo(endpoint, "%s/%s/_settings", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("PUT", endpoint->data, indexSettings);

	freeStringInfo(indexSettings);
	freeStringInfo(response);
	freeStringInfo(endpoint);

}


void elasticsearch_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping)
{
	char *properties;
	Datum PROPERTIES = CStringGetTextDatum("properties");
	StringInfo endpoint = makeStringInfo();
	StringInfo request = makeStringInfo();
	StringInfo response;
	char *pkey = lookup_primary_key(indexDescriptor->schemaName, indexDescriptor->tableName, false);
	Relation indexRel;

	if (pkey == NULL)
	{
		pkey = "__no primary key__";
		elog(WARNING, "No primary key detected for %s.%s, continuing anyway", indexDescriptor->schemaName, indexDescriptor->tableName);
	}

	properties = TextDatumGetCString(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(mapping), PROPERTIES));

	indexRel = index_open(indexDescriptor->indexRelid, RowExclusiveLock);
	appendStringInfo(request,
			"{"
  		    "   \"data\": {"
			"       \"_all\": { \"enabled\": true, \"analyzer\": \"phrase\" },"
			"       \"_source\": { \"enabled\": false },"
			"       \"_field_names\": { \"index\": \"no\", \"store\": false },"
			"       \"_parent\": { \"type\": \"xact\" },"
			"       \"_meta\": { \"primary_key\": \"%s\", \"noxact\": %s },"
			"       \"properties\" : %s"
			"   },"
			"   \"settings\": {"
			"      \"number_of_replicas\": %d"
			"   }"
			"}",
			pkey,
			(ZDBIndexOptionsGetNoXact(indexRel) == true) ? "true" : "false",
			properties,
			ZDBIndexOptionsGetNumberOfReplicas(indexRel));
	index_close(indexRel, RowExclusiveLock);

	appendStringInfo(endpoint, "%s/%s/_mapping/data", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("PUT", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(response);
}

void elasticsearch_dropIndex(ZDBIndexDescriptor *indexDescriptor)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo response = NULL;

	appendStringInfo(endpoint, "%s/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

	response = rest_call("DELETE", endpoint->data, NULL);
	freeStringInfo(endpoint);
	freeStringInfo(response);
}

void elasticsearch_refreshIndex(ZDBIndexDescriptor *indexDescriptor)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo response;

	appendStringInfo(endpoint, "%s/%s/_refresh", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("GET", endpoint->data, NULL);
	checkForRefreshError(response);
	
	freeStringInfo(endpoint);
	freeStringInfo(response);
}

ZDBSearchResponse *elasticsearch_searchIndex(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries, uint64 *nhits)
{
	StringInfo        query;
	StringInfo        endpoint = makeStringInfo();
	StringInfo        response;
	ZDBSearchResponse *hits;

	query = buildQuery(indexDescriptor, xid, cid, queries, nqueries, false);

	appendStringInfo(endpoint, "%s/%s/xact/_pgtid", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, query);
	if (response->data[0] != '\0')
		elog(ERROR, "%s", response->data);

	if (response->len < 1 + sizeof(uint64)) /* bounds checking on data returned from ES */
		elog(ERROR, "Elasticsearch didn't return enough data");

	memcpy(nhits, response->data + 1, sizeof(uint64));
	if (response->len < 1 + sizeof(uint64) + (*nhits * (sizeof(BlockNumber) + sizeof(OffsetNumber)))) /* more bounds checking */
		elog(ERROR, "Elasticsearch says there's %ld hits, but didn't return all of them, len=%d", *nhits, response->len);

	hits = palloc(sizeof(ZDBSearchResponse));
	hits->httpResponse = response;
	hits->hits         = (response->data + 1 + sizeof(uint64));

	memcpy(&hits->total_hits, response->data+1, sizeof(uint64));

	freeStringInfo(endpoint);
	freeStringInfo(query);

	return hits;
}

uint64 elasticsearch_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo response;
	uint64     nhits;
	Datum 	   countDatum;
	char 	  *countString;

	appendStringInfo(endpoint, "%s/%s/%s/_count", indexDescriptor->url, indexDescriptor->fullyQualifiedName, type_name);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("GET", endpoint->data, NULL);
	if (response->data[0] != '{')
		elog(ERROR, "%s", response->data);

	countDatum = DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("count"));
	countString = TextDatumGetCString(countDatum);

	nhits = (uint64) atol(countString);

	freeStringInfo(endpoint);
	freeStringInfo(response);

	return nhits;
}

uint64 elasticsearch_estimateCount(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char **queries, int nqueries)
{
	StringInfo query;
	StringInfo endpoint = makeStringInfo();
	StringInfo response;
	uint64     nhits;

	query = buildQuery(indexDescriptor, xid, cid, queries, nqueries, true);

	appendStringInfo(endpoint, "%s/%s/xact/_pgcount", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, query);
	if (response->data[0] == '{')
		elog(ERROR, "%s", response->data);

	nhits = (uint64) atol(response->data);

	freeStringInfo(endpoint);
	freeStringInfo(query);
	freeStringInfo(response);

	return nhits;
}

uint64 elasticsearch_estimateSelectivity(ZDBIndexDescriptor *indexDescriptor, char *user_query)
{
	StringInfo query    = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo response;
	uint64     nhits;

	if (indexDescriptor->options)
		appendStringInfo(query, "#options(%s) ", indexDescriptor->options);
	appendStringInfo(query, "#child<data>((%s))", user_query);

	appendStringInfo(endpoint, "%s/%s/xact/_pgcount?selectivity=true", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "&preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, query);
	if (response->data[0] == '{')
		elog(ERROR, "%s", response->data);

	nhits = (uint64) atol(response->data);

	freeStringInfo(endpoint);
	freeStringInfo(query);
	freeStringInfo(response);

	return nhits;
}

char *elasticsearch_tally(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *user_query, int64 max_terms, char *sort_order)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

	query = buildQuery(indexDescriptor, xid, cid, &user_query, 1, true);

	appendStringInfo(request, "#tally(%s, \"%s\", %ld, \"%s\") %s",
			fieldname,
			stem,
			max_terms,
			sort_order,
			query->data);

	appendStringInfo(endpoint, "%s/%s/data/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *range_spec, char *user_query)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

	query = buildQuery(indexDescriptor, xid, cid, &user_query, 1, true);

	appendStringInfo(request, "#range(%s, '%s') %s",
			fieldname,
			range_spec,
			query->data);

	appendStringInfo(endpoint, "%s/%s/data/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_significant_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *user_query, int64 max_terms)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

	query = buildQuery(indexDescriptor, xid, cid, &user_query, 1, true);

	appendStringInfo(request, "#significant_terms(%s, \"%s\", %ld) %s",
			fieldname,
			stem,
			max_terms,
			query->data);

	appendStringInfo(endpoint, "%s/%s/data/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_extended_stats(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *user_query)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

	query = buildQuery(indexDescriptor, xid, cid, &user_query, 1, true);

	appendStringInfo(request, "#extended_stats(%s) %s",
			fieldname,
			query->data);

	appendStringInfo(endpoint, "%s/%s/data/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}


char *elasticsearch_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *aggregate_query, char *user_query)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

	query = buildQuery(indexDescriptor, xid, cid, &user_query, 1, true);

	appendStringInfo(request, "%s %s", aggregate_query, query->data);

	appendStringInfo(endpoint, "%s/%s/data/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_suggest_terms(ZDBIndexDescriptor *indexDescriptor, TransactionId xid, CommandId cid, char *fieldname, char *stem, char *user_query, int64 max_terms)
{
    StringInfo request = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    query = buildQuery(indexDescriptor, xid, cid, &user_query, 1, true);

    appendStringInfo(request, "#suggest(%s, '%s', %ld) %s", fieldname, stem, max_terms, query->data);

    appendStringInfo(endpoint, "%s/%s/data/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    response = rest_call("POST", endpoint->data, request);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_getIndexMapping(ZDBIndexDescriptor *indexDescriptor)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo response;

	appendStringInfo(endpoint, "%s/%s/_mapping", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("GET", endpoint->data, NULL);

	freeStringInfo(endpoint);
	return TextDatumGetCString(DirectFunctionCall2(json_object_field, CStringGetTextDatum(response->data), CStringGetTextDatum(indexDescriptor->fullyQualifiedName)));
}

char *elasticsearch_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo request = makeStringInfo();
	StringInfo response;

	if (indexDescriptor->options)
		appendStringInfo(request, "#options(%s) ", indexDescriptor->options);

	appendStringInfo(endpoint, "%s/%s/_pgmapping/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName, fieldname);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(endpoint);
	freeStringInfo(request);
	return response->data;
}

char *elasticsearch_highlight(ZDBIndexDescriptor *indexDescriptor, char *query, char *documentJson)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo request = makeStringInfo();
	StringInfo response;
	char *pkey = lookup_primary_key(indexDescriptor->schemaName, indexDescriptor->tableName, true);

	appendStringInfo(request, "{\"query\":%s, \"primary_key\": \"%s\", \"documents\":%s}", query, pkey, documentJson);
	appendStringInfo(endpoint, "%s/%s/_zdbhighlighter", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(endpoint);
	freeStringInfo(request);
	return response->data;
}

void elasticsearch_freeSearchResponse(ZDBSearchResponse *searchResponse)
{
	freeStringInfo(searchResponse->httpResponse);
	pfree(searchResponse);
}

ZDBSearchResponse *elasticsearch_getPossiblyExpiredItems(ZDBIndexDescriptor *indexDescriptor, uint64 *nitems)
{
	StringInfo        endpoint     = makeStringInfo();
	StringInfo        request      = makeStringInfo();
	StringInfo        response;
	ZDBSearchResponse *items;

	appendStringInfo(endpoint, "%s/%s/data/_pgtid", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	appendStringInfo(request, "#parent<xact>(_xmax_is_committed = true OR _xmin_is_committed = false)");
	response = rest_call("POST", endpoint->data, request);
	if (response->data[0] != '\0')
		elog(ERROR, "%s", response->data);

	memcpy(nitems, response->data + 1, sizeof(uint64));

	items = palloc(sizeof(ZDBSearchResponse));
	items->httpResponse = response;
	items->hits         = (response->data + 1 + sizeof(uint64));

	freeStringInfo(endpoint);
	freeStringInfo(request);

	return items;
}


void elasticsearch_bulkDelete(ZDBIndexDescriptor *indexDescriptor, List *itemPointers, int nitems)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo request  = makeStringInfo();
	StringInfo response;
	ListCell *lc;

	appendStringInfo(endpoint, "%s/%s/_bulk?refresh=true", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

	foreach(lc, itemPointers)
	{
		ItemPointerData *item = lfirst(lc);

		appendStringInfo(request, "{\"delete\":{\"_id\":\"%d-%d\", \"_type\": \"xact\"}}\n", ItemPointerGetBlockNumber(item), ItemPointerGetOffsetNumber(item));
		appendStringInfo(request, "{\"delete\":{\"_id\":\"%d-%d\", \"_type\": \"data\"}}\n", ItemPointerGetBlockNumber(item), ItemPointerGetOffsetNumber(item));

		if (request->len >= indexDescriptor->batch_size)
		{
			response = rest_call("POST", endpoint->data, request);
			checkForBulkError(response, "delete");

			resetStringInfo(request);
			freeStringInfo(response);
		}
	}

	if (request->len > 0)
	{
		response = rest_call("POST", endpoint->data, request);
		checkForBulkError(response, "delete");
	}

	freeStringInfo(endpoint);
	freeStringInfo(request);
}

static void appendBatchInsertData(ZDBIndexDescriptor *indexDescriptor, ItemPointer ht_ctid, TransactionId xmin, TransactionId xmax, CommandId cmin, CommandId cmax, bool xmin_committed, bool xmax_committed, text *value, StringInfo bulk)
{
	/* the xact state */
	appendStringInfo(bulk, "{\"index\":{\"_id\":\"%d-%d\",\"_type\":\"xact\"}}\n", ItemPointerGetBlockNumber(ht_ctid), ItemPointerGetOffsetNumber(ht_ctid));
	appendStringInfo(bulk, "{"
					"\"_xmin\":%d,"
					"\"_xmax\":%d,"
					"\"_cmin\":%d,"
					"\"_cmax\":%d,"
					"\"_xmin_is_committed\":%s,"
					"\"_xmax_is_committed\":%s"
					"}\n",
			xmin,
			xmax,
			cmin,
			cmax,
			xmin_committed ? "true" : "false",
			xmax_committed ? "true" : "false"
	);

	/* the data */
	appendStringInfo(bulk, "{\"index\":{\"_id\":\"%d-%d\",\"_type\":\"data\",\"_parent\":\"%d-%d\"}}\n", ItemPointerGetBlockNumber(ht_ctid), ItemPointerGetOffsetNumber(ht_ctid), ItemPointerGetBlockNumber(ht_ctid), ItemPointerGetOffsetNumber(ht_ctid));
	appendBinaryStringInfoAndStripLineBreaks(bulk, VARDATA(value), VARSIZE(value) - VARHDRSZ);
	appendStringInfoCharMacro(bulk, '\n');
}

void elasticsearch_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, TransactionId xmin, TransactionId xmax, CommandId cmin, CommandId cmax, bool xmin_is_committed, bool xmax_is_committed, text *data)
{
	BatchInsertData *batch = lookup_batch_insert_data(indexDescriptor, true);

	appendBatchInsertData(indexDescriptor, ctid, xmin, xmax, cmin, cmax, xmin_is_committed, xmax_is_committed, data, batch->bulk);
	batch->nprocessed++;

	if (batch->bulk->len > indexDescriptor->batch_size)
	{
		StringInfo endpoint = makeStringInfo();
		int idx;

		/* don't ?refresh=true here as a full .refreshIndex() is called after batchInsertFinish() */
		appendStringInfo(endpoint, "%s/%s/_bulk", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

		if (batch->rest->available == 0)
			rest_multi_partial_cleanup(batch->rest, false, true);

		idx = rest_multi_call(batch->rest, "POST", endpoint->data, batch->bulk, true);
		if (idx < 0)
		{
			while(!rest_multi_is_available(batch->rest))
				CHECK_FOR_INTERRUPTS();

			rest_multi_partial_cleanup(batch->rest, false, true);
			idx = rest_multi_call(batch->rest, "POST", endpoint->data, batch->bulk, true);
			if (idx < 0)
				elog(ERROR, "Unable to add multicall after waiting");
		}

		/* reset the bulk StringInfo for the next batch of records */
		batch->bulk = makeStringInfo();

		batch->nrequests++;

		elog(LOG, "Indexed %d rows for %s", batch->nprocessed, indexDescriptor->fullyQualifiedName);
	}
}

void elasticsearch_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor)
{
	BatchInsertData *batch = lookup_batch_insert_data(indexDescriptor, false);

	if (batch)
	{
		if (batch->nrequests > 0)
		{
			/** wait for all outstanding HTTP requests to finish */
			while (!rest_multi_all_done(batch->rest)) {
				rest_multi_is_available(batch->rest);
				CHECK_FOR_INTERRUPTS();
			}
			rest_multi_partial_cleanup(batch->rest, true, false);
		}

		if (batch->bulk->len > 0)
		{
			StringInfo endpoint = makeStringInfo();
			StringInfo response;

			appendStringInfo(endpoint, "%s/%s/_bulk", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

			if (batch->nrequests == 0)
			{
				/*
				 * if this is the only request being made in this batch, then we'll ?refresh=true
				 * to avoid an additional round-trip to ES
				 */
				appendStringInfo(endpoint, "?refresh=true");
				response = rest_call("POST", endpoint->data, batch->bulk);
			}
			else
			{
				/*
				 * otherwise we'll do a full refresh below, so there's no need to do it here
				 */
				response = rest_call("POST", endpoint->data, batch->bulk);
			}
			checkForBulkError(response, "batch finish");
			freeStringInfo(response);
		}

		/*
		 * If this wasn't the only request being made in this batch
		 * then ask ES to refresh the index
		 */
		if (batch->nrequests > 0)
			elasticsearch_refreshIndex(indexDescriptor);

		freeStringInfo(batch->bulk);

		if (batch->nrequests > 0)
			elog(LOG, "Indexed %d rows in %d requests for %s", batch->nprocessed, batch->nrequests+1, indexDescriptor->fullyQualifiedName);

		batchInsertDataList = list_delete(batchInsertDataList, batch);
		pfree(batch);
	}
}

void elasticsearch_commitXactData(ZDBIndexDescriptor *indexDescriptor, List *xactData)
{
	ListCell   *lc;
	StringInfo endpoint = makeStringInfo();
	StringInfo bulk     = makeStringInfo();
	StringInfo response;
	int batchno = 1;

	foreach(lc, xactData)
	{
		ZDBCommitXactData *data = lfirst(lc);

		switch (data->type)
		{
			case ZDB_COMMIT_TYPE_NEW:
			{
				ZDBCommitNewXactData *new = (ZDBCommitNewXactData *) data;

				appendStringInfo(bulk,
						"{\"update\":{\"_id\":\"%d-%d\"}}\n"
								"{\"doc\":{\"_xmin_is_committed\":true}}\n",
						ItemPointerGetBlockNumber(new->header.ctid),
						ItemPointerGetOffsetNumber(new->header.ctid)
				);
			}
            break;

			case ZDB_COMMIT_TYPE_EXPIRED:
			{
				ZDBCommitExpiredXactData *expired = (ZDBCommitExpiredXactData *) data;

				appendStringInfo(bulk,
						"{\"update\":{\"_id\":\"%d-%d\"}}\n"
								"{\"doc\":{\"_xmax\":%d,\"_cmax\":%d,\"_xmax_is_committed\":true}}\n",
						ItemPointerGetBlockNumber(expired->header.ctid),
						ItemPointerGetOffsetNumber(expired->header.ctid),
						expired->xmax,
						expired->cmax
				);
			}
            break;

			case ZDB_COMMIT_TYPE_ALL:
				elog(ERROR, "Unexpected CommitXactDataType: ZDB_COMMIT_TYPE_ALL");
				break;
		}

		if (bulk->len > indexDescriptor->batch_size)
		{
			elog(LOG, "POSTING %d bytes for xact data during COMMIT, batch #%d", bulk->len, batchno);
			appendStringInfo(endpoint, "%s/%s/xact/_bulk", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
			response = rest_call("POST", endpoint->data, bulk);
			checkForBulkError(response, "xact");

			resetStringInfo(bulk);
			resetStringInfo(endpoint);
			freeStringInfo(response);
			batchno++;
		}
	}

	if (batchno > 0)
	{
		if (batchno > 1)
			elog(LOG, "POSTING final %d bytes for xact data during COMMIT, batch #%d", bulk->len, batchno);

		appendStringInfo(endpoint, "%s/%s/xact/_bulk?refresh=%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName, batchno ==1 ? "true" : "false");
		response = rest_call("POST", endpoint->data, bulk);
		checkForBulkError(response, "xact");
		freeStringInfo(response);
	}

	if (batchno > 1)
		elasticsearch_refreshIndex(indexDescriptor);

	freeStringInfo(bulk);
	freeStringInfo(endpoint);
}

void elasticsearch_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType)
{
	batchInsertDataList = NULL;
}
