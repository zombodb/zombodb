/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
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
#include "pgstat.h"
#include "miscadmin.h"
#include "access/genam.h"
#include "access/xact.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/snapmgr.h"

#include "rest/rest.h"
#include "util/zdbutils.h"

#include "elasticsearch.h"
#include "zdbseqscan.h"

#define MAX_LINKED_INDEXES 1024

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

static char **parse_linked_indices(char *schema, char *options, int *many) {
    size_t len       = strlen(options);
    size_t schemalen = strlen(schema);
    char   **indices = palloc(MAX_LINKED_INDEXES * sizeof(char *));
    int    i, x      = 0;

    for (i = 0; i < len; i++) {
        char ch = options[i];

        switch (ch) {
            case '<': {
                int j = (int) schemalen;

                if (x == MAX_LINKED_INDEXES)
                    elog(ERROR, "Too many linked indices.  Max is %d", MAX_LINKED_INDEXES);


                /* allocate buffer to store index name, schema qualified */
                indices[x] = palloc(schemalen + NAMEDATALEN + 2);   /* +2 for '.' and \0 */
                strcpy(indices[x], schema);
                indices[x][j++] = '.';

                while (options[++i] != '.');
                while (options[++i] != '>' && i < len) {
                    indices[x][j++] = options[i];
                }
                indices[x][j]   = '\0';

                x++;
            }
                break;

            default:
                break;
        }
    }

    *many = x;
    return indices;
}

static char *buildXidExclusionClause() {
	StringInfo sb = makeStringInfo();
	Snapshot snap = GetActiveSnapshot();
	int i;

	/*
     * exclude records by xid that we know we cannot see, which are
     *   a) anything greater than or equal to the snapshot's 'xmax'
     *      (but we can see ourself)
     */
	appendStringInfo(sb, "(_xid >= %lu AND _xid<>%lu) OR ", convert_xid(snap->xmax), ConvertedTopTransactionId);

	/*
     *   b) the xid of any currently running transaction
     */
	appendStringInfo(sb, "_xid:[[");
	for (i = 0; i<snap->xcnt; i++) {
		if(i>0) appendStringInfoChar(sb, ',');
		appendStringInfo(sb, "%lu", convert_xid(snap->xip[i]));
	}
	appendStringInfo(sb, "]]");

	return sb->data;
}

static StringInfo buildQuery(ZDBIndexDescriptor *desc, char **queries, int nqueries, bool useInvisibilityMap)
{
	StringInfo baseQuery = makeStringInfo();
    StringInfo ids;
	int i;

	if (desc->options)
		appendStringInfo(baseQuery, "#options(%s) ", desc->options);
	if (desc->fieldLists)
		appendStringInfo(baseQuery, "#field_lists(%s) ", desc->fieldLists);

    if (useInvisibilityMap) {
		char *xidExclusionClause = buildXidExclusionClause();

        if (desc->options != NULL) {
            int  index_cnt;
            char **indices = parse_linked_indices(desc->schemaName, desc->options, &index_cnt);

            for (i = 0; i < index_cnt; i++) {
                ZDBIndexDescriptor *tmp = zdb_alloc_index_descriptor_by_index_oid(DatumGetObjectId(DirectFunctionCall1(text_regclass, CStringGetTextDatum(indices[i]))));

                if (!tmp->ignoreVisibility) {
                    Relation rel;

                    rel = RelationIdGetRelation(tmp->heapRelid);
                    ids = find_invisible_ctids(rel);

                    if (ids->len > 0)
                        appendStringInfo(baseQuery, "#exclude<%s>(_id:[[%s]] OR (%s))", tmp->fullyQualifiedName, ids->data, xidExclusionClause);

                    freeStringInfo(ids);
                    RelationClose(rel);
                }
            }
        }

        if (!desc->ignoreVisibility) {
            Relation heapRel = RelationIdGetRelation(desc->heapRelid);

            ids = find_invisible_ctids(heapRel);
            if (ids->len > 0)
                appendStringInfo(baseQuery, "#exclude<%s>(_id:[[%s]] OR (%s))", desc->fullyQualifiedName, ids->data, xidExclusionClause);

            freeStringInfo(ids);
            RelationClose(heapRel);
        }
    }

    for (i = 0; i < nqueries; i++) {
        if (i > 0) appendStringInfo(baseQuery, " AND ");
        appendStringInfo(baseQuery, "(%s)", queries[i]);
    }

    return baseQuery;
}

static void checkForBulkError(StringInfo response, char *type) {
	text *errorsText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("errors")));
	if (errorsText == NULL)
		elog(ERROR, "Unexpected response from elasticsearch during %s: %s", type, response->data);
	else
	{
		char *errors = TextDatumGetCString(errorsText);
		if (strcmp(errors, "false") != 0)
			elog(ERROR, "Error updating %s data: %s", type, response->data);
		pfree(errors);
		pfree(errorsText);
	}
}

static void checkForRefreshError(StringInfo response) {
	Datum shards = DirectFunctionCall2(json_object_field, CStringGetTextDatum(response->data), CStringGetTextDatum("_shards"));
	text *failedText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, shards, CStringGetTextDatum("failed")));
	if (failedText == NULL)
		elog(ERROR, "Unexpected response from elasticsearch during _refresh: %s", response->data);
	else
	{
		char *errors = TextDatumGetCString(failedText);
		if (strcmp(errors, "0") != 0)
			elog(ERROR, "Error refresing: %s", response->data);
		pfree(errors);
		pfree(failedText);
	}
}

static void es_wait_for_index_availability(ZDBIndexDescriptor *indexDescriptor) {
    StringInfo endpoint = makeStringInfo();
    StringInfo response;
    char       *status;

	/* ask ES to wait for the health status of this index to be at least yellow.  Has default timeout of 30s */
	appendStringInfo(endpoint, "%s/_cluster/health/%s?wait_for_status=yellow", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("GET", endpoint->data, NULL);
	if (response->len == 0 || response->data[0] != '{')
		elog(ERROR, "Response from cluster health not in correct format: %s", response->data);

	status = TextDatumGetCString(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("status")));

	/* If the index isn't available, raise an error */
	if (status == NULL || (strcmp("green", status) != 0 && strcmp("yellow", status) != 0))
		elog(ERROR, "Index health indicates index is not available: %s", response->data);

    /* otherwise, it's all good */
	freeStringInfo(endpoint);
}

void elasticsearch_createNewIndex(ZDBIndexDescriptor *indexDescriptor, int shards, char *fieldProperties) {
    StringInfo endpoint      = makeStringInfo();
    StringInfo indexSettings = makeStringInfo();
    StringInfo response;
    char       *pkey         = lookup_primary_key(indexDescriptor->schemaName, indexDescriptor->tableName, false);

    if (pkey == NULL) {
        pkey = "__no primary key__";
        elog(WARNING, "No primary key detected for %s.%s, continuing anyway", indexDescriptor->schemaName, indexDescriptor->tableName);
    }

    appendStringInfo(indexSettings, "{"
            "   \"mappings\": {"
            "      \"data\": {"
            "          \"_source\": { \"enabled\": false },"
            "          \"_all\": { \"enabled\": true, \"analyzer\": \"phrase\" },"
            "          \"_field_names\": { \"index\": \"no\", \"store\": false },"
            "          \"_meta\": { \"primary_key\": \"%s\" },"
            "          \"date_detection\": false,"
            "          \"properties\" : %s"
            "      }"
            "   },"
            "   \"settings\": {"
            "      \"refresh_interval\": -1,"
            "      \"number_of_shards\": %d,"
            "      \"number_of_replicas\": 0,"
            "      \"analysis\": {"
            "         \"filter\": { %s },"
            "         \"char_filter\" : { %s },"
            "         \"tokenizer\" : { %s },"
            "         \"analyzer\": { %s }"
            "      }"
            "   }"
            "}", pkey, fieldProperties, shards, lookup_analysis_thing(CurrentMemoryContext, "zdb_filters"), lookup_analysis_thing(CurrentMemoryContext, "zdb_char_filters"), lookup_analysis_thing(CurrentMemoryContext, "zdb_tokenizers"), lookup_analysis_thing(CurrentMemoryContext, "zdb_analyzers"));

	appendStringInfo(endpoint, "%s/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("POST", endpoint->data, indexSettings);

	freeStringInfo(response);
    freeStringInfo(indexSettings);
    freeStringInfo(endpoint);
}

void elasticsearch_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor) {
    StringInfo endpoint      = makeStringInfo();
    StringInfo indexSettings = makeStringInfo();
    StringInfo response;
    Relation   indexRel;

    indexRel = RelationIdGetRelation(indexDescriptor->indexRelid);
    appendStringInfo(indexSettings, "{"
            "   \"index\": {"
			"      \"refresh_interval\":\"%s\","
            "      \"number_of_replicas\":%d"
            "   }"
            "}", ZDBIndexOptionsGetRefreshInterval(indexRel), ZDBIndexOptionsGetNumberOfReplicas(indexRel));
    RelationClose(indexRel);

	appendStringInfo(endpoint, "%s/%s/_settings", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("PUT", endpoint->data, indexSettings);

	freeStringInfo(response);

    es_wait_for_index_availability(indexDescriptor);

    freeStringInfo(indexSettings);
    freeStringInfo(endpoint);

}

void elasticsearch_updateMapping(ZDBIndexDescriptor *indexDescriptor, char *mapping) {
    char       *properties;
    Datum      PROPERTIES = CStringGetTextDatum("properties");
    StringInfo endpoint   = makeStringInfo();
    StringInfo request    = makeStringInfo();
    StringInfo response;
    char       *pkey      = lookup_primary_key(indexDescriptor->schemaName, indexDescriptor->tableName, false);
    Relation   indexRel;

    if (pkey == NULL) {
        pkey = "__no primary key__";
        elog(WARNING, "No primary key detected for %s.%s, continuing anyway", indexDescriptor->schemaName, indexDescriptor->tableName);
    }

    properties = TextDatumGetCString(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(mapping), PROPERTIES));

    indexRel = index_open(indexDescriptor->indexRelid, RowExclusiveLock);
    appendStringInfo(request, "{"
            "   \"data\": {"
            "       \"_all\": { \"enabled\": true, \"analyzer\": \"phrase\" },"
            "       \"_source\": { \"enabled\": false },"
            "       \"_field_names\": { \"index\": \"no\", \"store\": false },"
            "       \"_meta\": { \"primary_key\": \"%s\" },"
			"       \"date_detection\": false,"
            "       \"properties\" : %s"
            "   },"
            "   \"settings\": {"
			"      \"refresh_interval\":\"%s\","
            "      \"number_of_replicas\": %d"
            "   }"
            "}", pkey, properties, ZDBIndexOptionsGetRefreshInterval(indexRel), ZDBIndexOptionsGetNumberOfReplicas(indexRel));
    index_close(indexRel, RowExclusiveLock);

    appendStringInfo(endpoint, "%s/%s/_mapping/data", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("PUT", endpoint->data, request);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(response);
}

void elasticsearch_dropIndex(ZDBIndexDescriptor *indexDescriptor) {
    StringInfo endpoint = makeStringInfo();
    StringInfo response = NULL;

	appendStringInfo(endpoint, "%s/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("DELETE", endpoint->data, NULL);
	freeStringInfo(response);

    freeStringInfo(endpoint);
}

void elasticsearch_refreshIndex(ZDBIndexDescriptor *indexDescriptor) {
    StringInfo endpoint = makeStringInfo();
    StringInfo response;

	appendStringInfo(endpoint, "%s/%s/_refresh", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("GET", endpoint->data, NULL);
	checkForRefreshError(response);

	freeStringInfo(response);
    freeStringInfo(endpoint);
}

char *elasticsearch_multi_search(ZDBIndexDescriptor **descriptors, char **user_queries, int nqueries) {
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo response;
	int i;

	appendStringInfoChar(request, '[');
	for (i=0; i<nqueries; i++) {
		StringInfo query;
		char *indexName;
		char *preference;
		char *pkey;

		indexName  = descriptors[i]->fullyQualifiedName;
		preference = descriptors[i]->searchPreference;
		pkey       = lookup_primary_key(descriptors[i]->schemaName, descriptors[i]->tableName, false);
        query      = buildQuery(descriptors[i], &user_queries[i], 1, true);

		if (!preference) preference = "null";

		if (i>0)
			appendStringInfoChar(request, ',');

		appendStringInfoChar(request, '{');

		appendStringInfo(request, "\"indexName\":");
		escape_json(request, indexName);

		appendStringInfo(request, ",\"query\":");
		escape_json(request, query->data);

		if (preference) {
			appendStringInfo(request, ",\"preference\":");
			escape_json(request, preference);
		}

		if (pkey) {
			appendStringInfo(request, ", \"pkey\":");
			escape_json(request, pkey);
		}

		appendStringInfoChar(request, '}');
	}
	appendStringInfoChar(request, ']');

    appendStringInfo(endpoint, "%s/%s/_zdbmsearch", descriptors[0]->url, descriptors[0]->fullyQualifiedName);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);

	return response->data;
}


ZDBSearchResponse *elasticsearch_searchIndex(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries, uint64 *nhits)
{
	StringInfo        query;
	StringInfo        endpoint = makeStringInfo();
	StringInfo        response;
	ZDBSearchResponse *hits;
	ZDBScore		  max_score;
	bool              useInvisibilityMap = strstr(queries[0], "#expand") != NULL || indexDescriptor->options != NULL;

    appendStringInfo(endpoint, "%s/%s/_pgtid", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	query = buildQuery(indexDescriptor, queries, nqueries, useInvisibilityMap);
	response = rest_call("POST", endpoint->data, query);

	if (response->data[0] != '\0')
		elog(ERROR, "%s", response->data);

	if (response->len < 1 + sizeof(uint64) + sizeof(float4)) /* bounds checking on data returned from ES */
		elog(ERROR, "Elasticsearch didn't return enough data");

    /* get the number of hits and max score from the response data */
	memcpy(nhits, response->data + 1, sizeof(uint64));
	memcpy(&max_score, response->data + 1 + sizeof(uint64), sizeof(float4));

    /* and make sure we have the specified number of hits in the response data */
	if (response->len != 1 + sizeof(uint64) + sizeof(float4) + (*nhits * (sizeof(BlockNumber) + sizeof(OffsetNumber) + sizeof(float4)))) /* more bounds checking */
		elog(ERROR, "Elasticsearch says there's %ld hits, but didn't return all of them, len=%d", *nhits, response->len);

	hits = palloc(sizeof(ZDBSearchResponse));
	hits->httpResponse = response;
	hits->hits         = (response->data + 1 + sizeof(uint64) + sizeof(float4));
	hits->total_hits   = *nhits;
	hits->max_score    = max_score.fscore;

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

uint64 elasticsearch_estimateCount(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries)
{
	StringInfo query;
	StringInfo endpoint = makeStringInfo();
	StringInfo response;
	uint64     nhits;

    appendStringInfo(endpoint, "%s/%s/_pgcount", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	query = buildQuery(indexDescriptor, queries, nqueries, true);
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
	StringInfo query;
	StringInfo endpoint = makeStringInfo();
	StringInfo response;
	uint64     nhits;

    query = buildQuery(indexDescriptor, &user_query, 1, false);

    appendStringInfo(endpoint, "%s/%s/_pgcount?selectivity=true", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
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

char *elasticsearch_tally(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *user_query, int64 max_terms, char *sort_order)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

	appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
	appendStringInfo(request, "#tally(%s, \"%s\", %ld, \"%s\") %s", fieldname, stem, max_terms, sort_order, query->data);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *range_spec, char *user_query)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	query = buildQuery(indexDescriptor, &user_query, 1, true);
	appendStringInfo(request, "#range(%s, '%s') %s", fieldname, range_spec, query->data);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_significant_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *user_query, int64 max_terms)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	query = buildQuery(indexDescriptor, &user_query, 1, true);
	appendStringInfo(request, "#significant_terms(%s, \"%s\", %ld) %s", fieldname, stem, max_terms, query->data);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_extended_stats(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *user_query)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	query = buildQuery(indexDescriptor, &user_query, 1, true);
	appendStringInfo(request, "#extended_stats(%s) %s", fieldname, query->data);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}


char *elasticsearch_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, char *aggregate_query, char *user_query)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo query;
	StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	query = buildQuery(indexDescriptor, &user_query, 1, true);
	appendStringInfo(request, "%s %s", aggregate_query, query->data);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);
	freeStringInfo(query);

	return response->data;
}

char *elasticsearch_suggest_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *user_query, int64 max_terms)
{
    StringInfo request = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	query = buildQuery(indexDescriptor, &user_query, 1, true);
	appendStringInfo(request, "#suggest(%s, '%s', %ld) %s", fieldname, stem, max_terms, query->data);
    response = rest_call("POST", endpoint->data, request);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_termlist(ZDBIndexDescriptor *descriptor, char *fieldname, char *prefix, char *startat, uint32 size)
{
	StringInfo request = makeStringInfo();
	StringInfo endpoint = makeStringInfo();
	StringInfo response;

	appendStringInfo(request, "{\"fieldname\":");
	escape_json(request, fieldname);
	appendStringInfo(request, ", \"prefix\":");
	escape_json(request, prefix);
	if (startat != NULL) {
		appendStringInfo(request, ", \"startAt\":");
		escape_json(request, startat);
	}
	appendStringInfo(request, ", \"size\":%d}", size);

	appendStringInfo(endpoint, "%s/%s/_zdbtermlist", descriptor->url, descriptor->fullyQualifiedName);
	response = rest_call("POST", endpoint->data, request);

	freeStringInfo(request);
	freeStringInfo(endpoint);

	return response->data;
}


char *elasticsearch_getIndexMapping(ZDBIndexDescriptor *indexDescriptor) {
    char *indexName = palloc(strlen(indexDescriptor->fullyQualifiedName) + 3);
    StringInfo endpoint = makeStringInfo();
    StringInfo response;

    sprintf(indexName, "%s", indexDescriptor->fullyQualifiedName);
    appendStringInfo(endpoint, "%s/%s/_mapping", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("GET", endpoint->data, NULL);

    freeStringInfo(endpoint);
    return TextDatumGetCString(DirectFunctionCall2(json_object_field, CStringGetTextDatum(response->data), CStringGetTextDatum(indexName)));
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

char *elasticsearch_analyzeText(ZDBIndexDescriptor *indexDescriptor, char *analyzerName, char *data)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo request = makeStringInfo();
	StringInfo response;

    appendStringInfo(request, "%s", data);
    appendStringInfo(endpoint, "%s/%s/_analyze?analyzer=%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName, analyzerName);
    response = rest_call("GET", endpoint->data, request);

	freeStringInfo(endpoint);
	freeStringInfo(request);
	return response->data;

}

char *elasticsearch_highlight(ZDBIndexDescriptor *indexDescriptor, char *user_query, zdb_json documentJson)
{
	StringInfo endpoint = makeStringInfo();
	StringInfo request = makeStringInfo();
	StringInfo response;
	char *pkey = lookup_primary_key(indexDescriptor->schemaName, indexDescriptor->tableName, true);

	appendStringInfo(request, "{\"query\":%s, \"primary_key\": \"%s\", \"documents\":%s", user_query, pkey, documentJson);
	if (indexDescriptor->fieldLists)
		appendStringInfo(request, ", \"field_lists\":\"%s\"", indexDescriptor->fieldLists);
	appendStringInfoChar(request, '}');

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

    appendStringInfo(endpoint, "%s/%s/_pgtid", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	if (indexDescriptor->searchPreference != NULL)
		appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

	response = rest_call("POST", endpoint->data, request);
	if (response->data[0] != '\0')
		elog(ERROR, "%s", response->data);

	memcpy(nitems, response->data + 1, sizeof(uint64));

	items = palloc(sizeof(ZDBSearchResponse));
	items->httpResponse = response;
	items->hits         = (response->data + 1 + sizeof(uint64) + sizeof(float4));

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

	appendStringInfo(endpoint, "%s/%s/data/_bulk", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

	foreach(lc, itemPointers)
	{
		ItemPointer item = lfirst(lc);

		appendStringInfo(request, "{\"delete\":{\"_id\":\"%d-%d\"}}\n", ItemPointerGetBlockNumber(item), ItemPointerGetOffsetNumber(item));

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

	elasticsearch_refreshIndex(indexDescriptor);

	freeStringInfo(endpoint);
	freeStringInfo(request);
}

static void appendBatchInsertData(ZDBIndexDescriptor *indexDescriptor, ItemPointer ht_ctid, text *value, StringInfo bulk)
{
	/* the data */
	appendStringInfo(bulk, "{\"index\":{\"_id\":\"%d-%d\"}}\n", ItemPointerGetBlockNumber(ht_ctid), ItemPointerGetOffsetNumber(ht_ctid));
	appendBinaryStringInfoAndStripLineBreaks(bulk, VARDATA(value), VARSIZE(value) - VARHDRSZ);

	/* backup to remove the last '}' of the value json, so that we can... */
	while (bulk->data[bulk->len] != '}')
		bulk->len--;

	/* ...append our transaction id to the json */
	appendStringInfo(bulk, ",\"_xid\":%lu}\n", ConvertedTopTransactionId);
}

void elasticsearch_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, text *data)
{
	BatchInsertData *batch = lookup_batch_insert_data(indexDescriptor, true);

	appendBatchInsertData(indexDescriptor, ctid, data, batch->bulk);
	batch->nprocessed++;

	if (batch->bulk->len > indexDescriptor->batch_size)
	{
		StringInfo endpoint = makeStringInfo();
		int idx;

		/* don't ?refresh=true here as a full .refreshIndex() is called after batchInsertFinish() */
		appendStringInfo(endpoint, "%s/%s/data/_bulk", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

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

			appendStringInfo(endpoint, "%s/%s/data/_bulk", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

			if (batch->nrequests == 0)
			{
				/*
				 * if this is the only request being made in this batch, then we'll ?refresh=true
				 * to avoid an additional round-trip to ES, but only if a) we're not in batch mode
				 * and b) if the index refresh interval is -1
				 */
				if (!zdb_batch_mode_guc) {
					if (strcmp("-1", indexDescriptor->refreshInterval) == 0) {
						appendStringInfo(endpoint, "?refresh=true");
					}
				}

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
         * then ask ES to refresh the index, but only if a) we're not in batch mode
         * and b) if the index refresh interval is -1
         */
		if (batch->nrequests > 0) {
			if (!zdb_batch_mode_guc) {
				if (strcmp("-1", indexDescriptor->refreshInterval) == 0) {
					elasticsearch_refreshIndex(indexDescriptor);
				}
			}
		}

		freeStringInfo(batch->bulk);

		if (batch->nrequests > 0)
			elog(LOG, "Indexed %d rows in %d requests for %s", batch->nprocessed, batch->nrequests+1, indexDescriptor->fullyQualifiedName);

		batchInsertDataList = list_delete(batchInsertDataList, batch);
		pfree(batch);
	}
}

void elasticsearch_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType) {
    batchInsertDataList = NULL;
}
