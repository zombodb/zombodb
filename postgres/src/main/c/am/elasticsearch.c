/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2017 ZomboDB, LLC
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
#include <stdlib.h>
#include "postgres.h"

#include "fmgr.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

#include "rest/rest.h"
#include "util/zdbutils.h"

#include "elasticsearch.h"
#include "zdbseqscan.h"
#include "zdb_interface.h"

#define SECONDARY_TYPES_MAPPING \
"      \"state\": {"\
"          \"_source\": { \"enabled\": false },"\
"          \"_routing\": { \"required\": true },"\
"          \"_all\": { \"enabled\": false },"\
"          \"_field_names\": { \"index\": \"no\", \"store\": false },"\
"          \"date_detection\": false,"\
"          \"properties\": { \"_ctid\":{\"type\":\"string\",\"index\":\"not_analyzed\"} }"\
"      },"\
"      \"deleted\": {"\
"          \"_source\": { \"enabled\": false },"\
"          \"_all\": { \"enabled\": false },"\
"          \"_field_names\": { \"index\": \"no\", \"store\": false },"\
"          \"properties\": {"\
"              \"_deleting_xid\": { \"type\": \"long\", \"index\": \"not_analyzed\" }"\
"          }"\
"      },"\
"      \"committed\": {"\
"          \"_source\": { \"enabled\": false },"\
"          \"_routing\": { \"required\": true },"\
"          \"_all\": { \"enabled\": false },"\
"          \"_field_names\": { \"index\": \"no\", \"store\": false },"\
"          \"properties\": {"\
"             \"_zdb_committed_xid\": { \"type\": \"long\",\"index\":\"not_analyzed\" }"\
"          }"\
"      }"

typedef struct {
    ZDBIndexDescriptor *indexDescriptor;
    MultiRestState     *rest;
    PostDataEntry      *bulk;
    int                nprocessed;
    int                nrequests;
    int                nrecs;

    StringInfo         pool[MAX_CURL_HANDLES];
} BatchInsertData;


static List *batchInsertDataList = NULL;

static BatchInsertData *lookup_batch_insert_data(ZDBIndexDescriptor *indexDescriptor, bool create) {
    BatchInsertData *data = NULL;
    ListCell        *lc;

    foreach(lc, batchInsertDataList) {
        BatchInsertData *tmp = lfirst(lc);

        if (tmp->indexDescriptor->indexRelid == indexDescriptor->indexRelid) {
            data = tmp;
            break;
        }
    }

    if (!data && create) {
        int i;
        MemoryContext oldcontext = MemoryContextSwitchTo(TopTransactionContext);

        data = palloc0(sizeof(BatchInsertData));
        data->indexDescriptor = indexDescriptor;
        data->rest            = palloc0(sizeof(MultiRestState));
        data->bulk            = NULL;
        data->nprocessed      = 0;
        data->nrequests       = 0;

        for (i=0; i<indexDescriptor->bulk_concurrency+1; i++)
            data->pool[i] = makeStringInfo();

        batchInsertDataList = lappend(batchInsertDataList, data);
        MemoryContextSwitchTo(oldcontext);

        rest_multi_init(data->rest, indexDescriptor->bulk_concurrency);
        data->rest->pool = data->pool;
    }

    return data;
}

static StringInfo buildQuery(ZDBIndexDescriptor *desc, char **queries, int nqueries, bool useInvisibilityMap) {
    StringInfo baseQuery = makeStringInfo();
    int        i;

    if (desc->options)
        appendStringInfo(baseQuery, "#options(%s) ", desc->options);
    if (desc->fieldLists)
        appendStringInfo(baseQuery, "#field_lists(%s) ", desc->fieldLists);

    if (!zdb_ignore_visibility_guc && useInvisibilityMap) {
        Snapshot snapshot = GetActiveSnapshot();

        appendStringInfo(baseQuery, "#visibility(%lu, %lu, %lu, [", convert_xid(GetCurrentTransactionIdIfAny()), convert_xid(snapshot->xmin), convert_xid(snapshot->xmax));
        if (snapshot->xcnt > 0) {
            for (i = 0; i < snapshot->xcnt; i++) {
                if (i > 0) appendStringInfoChar(baseQuery, ',');
                appendStringInfo(baseQuery, "%lu", convert_xid(snapshot->xip[i]));
            }
        }
        appendStringInfo(baseQuery, "])");
    }

    for (i = 0; i < nqueries; i++) {
        if (i > 0) appendStringInfo(baseQuery, " AND ");
        appendStringInfo(baseQuery, "(%s)", queries[i]);
    }

    return baseQuery;
}

static void checkForBulkError(StringInfo response, char *type) {
    if (strstr(response->data, "errors") != NULL) {
        text *errorsText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("errors")));
        if (errorsText == NULL)
            elog(ERROR, "Unexpected response from elasticsearch during %s: %s", type, response->data);
        else {
            char *errors = TextDatumGetCString(errorsText);
            if (strcmp(errors, "false") != 0)
                elog(ERROR, "Error updating %s data: %s", type, response->data);
            pfree(errors);
            pfree(errorsText);
        }
    }
}

static void checkForRefreshError(StringInfo response) {
    Datum shards      = DirectFunctionCall2(json_object_field, CStringGetTextDatum(response->data), CStringGetTextDatum("_shards"));
    text  *failedText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, shards, CStringGetTextDatum("failed")));
    if (failedText == NULL)
        elog(ERROR, "Unexpected response from elasticsearch during _refresh: %s", response->data);
    else {
        char *errors = TextDatumGetCString(failedText);
        if (strcmp(errors, "0") != 0)
            elog(ERROR, "Error refreshing: %s", response->data);
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
    response = rest_call("GET", endpoint->data, NULL, indexDescriptor->compressionLevel);
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

    appendStringInfo(indexSettings, "{"
            "   \"mappings\": {"
            "      \"data\": {"
            "          \"_source\": { \"enabled\": false },"
            "          \"_routing\": { \"required\": true },"
            "          \"_all\": { \"enabled\": true, \"analyzer\": \"phrase\" },"
            "          \"_field_names\": { \"index\": \"no\", \"store\": false },"
            "          \"_meta\": { \"primary_key\": \"%s\", \"always_resolve_joins\": %s },"
            "          \"date_detection\": false,"
            "          \"properties\" : %s"
            "      },"
			SECONDARY_TYPES_MAPPING
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
            "   }",
					 indexDescriptor->pkeyFieldname, indexDescriptor->alwaysResolveJoins ? "true"
                                                           : "false", fieldProperties, shards, lookup_analysis_thing(CurrentMemoryContext, "zdb_filters"), lookup_analysis_thing(CurrentMemoryContext, "zdb_char_filters"), lookup_analysis_thing(CurrentMemoryContext, "zdb_tokenizers"), lookup_analysis_thing(CurrentMemoryContext, "zdb_analyzers"));
	if (indexDescriptor->alias != NULL) {
		appendStringInfo(indexSettings, ", \"aliases\": { \"%s\": {} }", indexDescriptor->alias);
	}
	appendStringInfoChar(indexSettings, '}');

    appendStringInfo(endpoint, "%s/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("POST", endpoint->data, indexSettings, indexDescriptor->compressionLevel);

    freeStringInfo(response);
    freeStringInfo(indexSettings);
    freeStringInfo(endpoint);
}

void elasticsearch_finalizeNewIndex(ZDBIndexDescriptor *indexDescriptor, HTAB *committedXids) {
	HASH_SEQ_STATUS seq;
	TransactionId *xid;
    StringInfo endpoint      = makeStringInfo();
	StringInfo request       = makeStringInfo();
    StringInfo indexSettings = makeStringInfo();
    StringInfo response;
    Relation   indexRel;

	/*
	 * push out all committed transaction ids to ES
	 */
	hash_seq_init(&seq, committedXids);
	while ( (xid = hash_seq_search(&seq)) != NULL) {
		uint64 convertedXid = convert_xid(*xid);

		if (request->len > 0)
			appendStringInfoChar(request, '\n');
		appendStringInfo(request, "%lu", convertedXid);
	}
    if (request->len > 0) {
        appendStringInfo(endpoint, "%s/%s/_zdbxid?refresh=true", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
        response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);
        checkForBulkError(response, "bulk committed xid");
    }
    freeStringInfo(request);

	/*
	 * set various index settings to make it live
	 */
    indexRel = RelationIdGetRelation(indexDescriptor->indexRelid);
    appendStringInfo(indexSettings, "{"
            "   \"index\": {"
            "      \"refresh_interval\":\"%s\","
            "      \"number_of_replicas\":%d"
            "   }"
            "}", ZDBIndexOptionsGetRefreshInterval(indexRel), ZDBIndexOptionsGetNumberOfReplicas(indexRel));
    RelationClose(indexRel);

	resetStringInfo(endpoint);
    appendStringInfo(endpoint, "%s/%s/_settings", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("PUT", endpoint->data, indexSettings, indexDescriptor->compressionLevel);

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

    properties = TextDatumGetCString(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(mapping), PROPERTIES));

    indexRel = RelationIdGetRelation(indexDescriptor->indexRelid);

    /*
     * First, update the mapping.  It's important that we include all the same properties
     * as when we created the index, otherwise they'll be un-set by this call
     */
    appendStringInfo(request, "{"\
            "   \"data\": {"
            "      \"_source\": { \"enabled\": false },"
            "      \"_all\": { \"enabled\": true, \"analyzer\": \"phrase\" },"
            "      \"_field_names\": { \"index\": \"no\", \"store\": false },"
            "      \"_meta\": { \"primary_key\": \"%s\", \"always_resolve_joins\": %s },"
            "      \"date_detection\": false,"
            "      \"properties\" : %s"
            "    },"
			SECONDARY_TYPES_MAPPING
            "}", pkey, indexDescriptor->alwaysResolveJoins ? "true" : "false", properties);

    appendStringInfo(endpoint, "%s/%s/_mapping/data", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("PUT", endpoint->data, request, indexDescriptor->compressionLevel);
    freeStringInfo(response);

    /*
     * Second, update the index settings we can change dynamically
     */
    resetStringInfo(request);
    resetStringInfo(endpoint);

    appendStringInfo(request, "{"
            "   \"settings\": {"
            "      \"refresh_interval\": \"%s\","
            "      \"number_of_replicas\": %d"
            "   }"
            "}", ZDBIndexOptionsGetRefreshInterval(indexRel), ZDBIndexOptionsGetNumberOfReplicas(indexRel));

    appendStringInfo(endpoint, "%s/%s/_settings", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("PUT", endpoint->data, request, indexDescriptor->compressionLevel);
    freeStringInfo(response);

    RelationClose(indexRel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
}

char *elasticsearch_dumpQuery(ZDBIndexDescriptor *indexDescriptor, char *userQuery) {
    StringInfo query;
    StringInfo endpoint           = makeStringInfo();
    StringInfo response;
    bool       useInvisibilityMap = strstr(userQuery, "#expand") != NULL || indexDescriptor->options != NULL;

    appendStringInfo(endpoint, "%s/%s/_zdbquery", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query    = buildQuery(indexDescriptor, &userQuery, 1, useInvisibilityMap);
    response = rest_call("POST", endpoint->data, query, indexDescriptor->compressionLevel);

    freeStringInfo(query);
    freeStringInfo(endpoint);

    return response->data;
}

void elasticsearch_dropIndex(ZDBIndexDescriptor *indexDescriptor) {
    StringInfo endpoint = makeStringInfo();
    StringInfo response = NULL;

    appendStringInfo(endpoint, "%s/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("DELETE", endpoint->data, NULL, indexDescriptor->compressionLevel);
    freeStringInfo(response);
}

void elasticsearch_refreshIndex(ZDBIndexDescriptor *indexDescriptor) {
    if (!zdb_batch_mode_guc) {
        if (strcmp("-1", indexDescriptor->refreshInterval) == 0) {
            StringInfo endpoint = makeStringInfo();
            StringInfo response;

            elog(ZDB_LOG_LEVEL, "[zombodb] Refreshing index %s", indexDescriptor->fullyQualifiedName);
            appendStringInfo(endpoint, "%s/%s/_refresh", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
            response = rest_call("GET", endpoint->data, NULL, indexDescriptor->compressionLevel);
            checkForRefreshError(response);

            freeStringInfo(response);
            freeStringInfo(endpoint);
        }
    }
}

char *elasticsearch_multi_search(ZDBIndexDescriptor **descriptors, char **user_queries, int nqueries) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo response;
    int        i;

    appendStringInfoChar(request, '[');
    for (i = 0; i < nqueries; i++) {
        StringInfo query;
        char       *indexName;
        char       *preference;
        char       *pkey;

        indexName  = descriptors[i]->fullyQualifiedName;
        preference = descriptors[i]->searchPreference;
        pkey       = lookup_primary_key(descriptors[i]->schemaName, descriptors[i]->tableName, true);
        query      = buildQuery(descriptors[i], &user_queries[i], 1, true);

        if (!preference) preference = "null";

        if (i > 0)
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
    response = rest_call("POST", endpoint->data, request, descriptors[0]->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);

    return response->data;
}


ZDBSearchResponse *elasticsearch_searchIndex(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries, uint64 *nhits) {
    StringInfo        query;
    StringInfo        endpoint           = makeStringInfo();
    StringInfo        response;
    ZDBSearchResponse *hits;
    ZDBScore          max_score;
    bool              useInvisibilityMap = strstr(queries[0], "#expand") != NULL || indexDescriptor->options != NULL;

    appendStringInfo(endpoint, "%s/%s/_pgtid", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query    = buildQuery(indexDescriptor, queries, nqueries, useInvisibilityMap);
    response = rest_call("POST", endpoint->data, query, indexDescriptor->compressionLevel);

    if (response->data[0] != '\0')
        elog(ERROR, "%s", response->data);

    if (response->len < 1 + sizeof(uint64) + sizeof(float4)) /* bounds checking on data returned from ES */
        elog(ERROR, "Elasticsearch didn't return enough data");

    /* get the number of hits and max score from the response data */
    memcpy(nhits, response->data + 1, sizeof(uint64));
    memcpy(&max_score, response->data + 1 + sizeof(uint64), sizeof(float4));

    /* and make sure we have the specified number of hits in the response data */
    if (response->len != 1 + sizeof(uint64) + sizeof(float4) + (*nhits * (sizeof(BlockNumber) + sizeof(OffsetNumber) +
                                                                          sizeof(float4)))) /* more bounds checking */
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

uint64 elasticsearch_actualIndexRecordCount(ZDBIndexDescriptor *indexDescriptor, char *type_name) {
    StringInfo endpoint = makeStringInfo();
    StringInfo response;
    uint64     nhits;
    Datum      countDatum;
    char       *countString;

    appendStringInfo(endpoint, "%s/%s/%s/_count", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName, type_name);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    response = rest_call("GET", endpoint->data, NULL, indexDescriptor->compressionLevel);
    if (response->data[0] != '{')
        elog(ERROR, "%s", response->data);

    countDatum  = DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("count"));
    countString = TextDatumGetCString(countDatum);

    nhits = (uint64) atol(countString);

    freeStringInfo(endpoint);
    freeStringInfo(response);

    return nhits;
}

uint64 elasticsearch_estimateCount(ZDBIndexDescriptor *indexDescriptor, char **queries, int nqueries) {
    StringInfo query;
    StringInfo endpoint = makeStringInfo();
    StringInfo response;
    uint64     nhits;

    appendStringInfo(endpoint, "%s/%s/_pgcount", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query    = buildQuery(indexDescriptor, queries, nqueries, true);
    response = rest_call("POST", endpoint->data, query, indexDescriptor->compressionLevel);
    if (response->data[0] == '{')
        elog(ERROR, "%s", response->data);

    nhits = (uint64) atol(response->data);

    freeStringInfo(endpoint);
    freeStringInfo(query);
    freeStringInfo(response);
    return nhits;
}

uint64 elasticsearch_estimateSelectivity(ZDBIndexDescriptor *indexDescriptor, char *user_query) {
    StringInfo query;
    StringInfo endpoint = makeStringInfo();
    StringInfo response;
    uint64     nhits;

    query = buildQuery(indexDescriptor, &user_query, 1, false);

    appendStringInfo(endpoint, "%s/%s/_pgcount?selectivity=true", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    response = rest_call("POST", endpoint->data, query, indexDescriptor->compressionLevel);
    if (response->data[0] == '{')
        elog(ERROR, "%s", response->data);

    nhits = (uint64) atol(response->data);

    freeStringInfo(endpoint);
    freeStringInfo(query);
    freeStringInfo(response);

    return nhits;
}

char *elasticsearch_tally(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *user_query, int64 max_terms, char *sort_order, int shard_size) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
    appendStringInfo(request, "#tally(%s, \"%s\", %ld, \"%s\", %d) %s", fieldname, stem, max_terms, sort_order, shard_size, query->data);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_rangeAggregate(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *range_spec, char *user_query) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
    appendStringInfo(request, "#range(%s, '%s') %s", fieldname, range_spec, query->data);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_significant_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *user_query, int64 max_terms) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
    appendStringInfo(request, "#significant_terms(%s, \"%s\", %ld) %s", fieldname, stem, max_terms, query->data);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_extended_stats(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *user_query) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
    appendStringInfo(request, "#extended_stats(%s) %s", fieldname, query->data);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}


char *elasticsearch_arbitrary_aggregate(ZDBIndexDescriptor *indexDescriptor, char *aggregate_query, char *user_query) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
    appendStringInfo(request, "%s %s", aggregate_query, query->data);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_json_aggregate(ZDBIndexDescriptor *indexDescriptor, zdb_json json_agg, char *user_query) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
    appendStringInfo(request, "#json_agg(%s) %s ", json_agg, query->data);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_suggest_terms(ZDBIndexDescriptor *indexDescriptor, char *fieldname, char *stem, char *user_query, int64 max_terms) {
    StringInfo request  = makeStringInfo();
    StringInfo endpoint = makeStringInfo();
    StringInfo query;
    StringInfo response;

    appendStringInfo(endpoint, "%s/%s/_pgagg", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    if (indexDescriptor->searchPreference != NULL)
        appendStringInfo(endpoint, "?preference=%s", indexDescriptor->searchPreference);

    query = buildQuery(indexDescriptor, &user_query, 1, true);
    appendStringInfo(request, "#suggest(%s, '%s', %ld) %s", fieldname, stem, max_terms, query->data);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);
    freeStringInfo(query);

    return response->data;
}

char *elasticsearch_termlist(ZDBIndexDescriptor *descriptor, char *fieldname, char *prefix, char *startat, uint32 size) {
    StringInfo request  = makeStringInfo();
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
    response = rest_call("POST", endpoint->data, request, descriptor->compressionLevel);

    freeStringInfo(request);
    freeStringInfo(endpoint);

    return response->data;
}


char *elasticsearch_getIndexMapping(ZDBIndexDescriptor *indexDescriptor) {
    char       *indexName = palloc(strlen(indexDescriptor->fullyQualifiedName) + 3);
    StringInfo endpoint   = makeStringInfo();
    StringInfo response;

    sprintf(indexName, "%s", indexDescriptor->fullyQualifiedName);
    appendStringInfo(endpoint, "%s/%s/_mapping", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    response = rest_call("GET", endpoint->data, NULL, indexDescriptor->compressionLevel);

    freeStringInfo(endpoint);
    return TextDatumGetCString(DirectFunctionCall2(json_object_field, CStringGetTextDatum(response->data), CStringGetTextDatum(indexName)));
}

char *elasticsearch_describeNestedObject(ZDBIndexDescriptor *indexDescriptor, char *fieldname) {
    StringInfo endpoint = makeStringInfo();
    StringInfo request  = makeStringInfo();
    StringInfo response;

    if (indexDescriptor->options)
        appendStringInfo(request, "#options(%s) ", indexDescriptor->options);

    appendStringInfo(endpoint, "%s/%s/_pgmapping/%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName, fieldname);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(endpoint);
    freeStringInfo(request);
    return response->data;
}

char *elasticsearch_analyzeText(ZDBIndexDescriptor *indexDescriptor, char *analyzerName, char *data) {
    StringInfo endpoint = makeStringInfo();
    StringInfo request  = makeStringInfo();
    StringInfo response;

    appendStringInfo(request, "%s", data);
    appendStringInfo(endpoint, "%s/%s/_analyze?analyzer=%s", indexDescriptor->url, indexDescriptor->fullyQualifiedName, analyzerName);
    response = rest_call("GET", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(endpoint);
    freeStringInfo(request);
    return response->data;

}

char *elasticsearch_highlight(ZDBIndexDescriptor *indexDescriptor, char *user_query, zdb_json documentJson) {
    StringInfo endpoint = makeStringInfo();
    StringInfo request  = makeStringInfo();
    StringInfo response;
    char       *pkey    = lookup_primary_key(indexDescriptor->schemaName, indexDescriptor->tableName, true);

    appendStringInfo(request, "{\"query\":%s, \"primary_key\": \"%s\", \"documents\":%s", user_query, pkey, documentJson);
    if (indexDescriptor->fieldLists)
        appendStringInfo(request, ", \"field_lists\":\"%s\"", indexDescriptor->fieldLists);
    appendStringInfoChar(request, '}');

    appendStringInfo(endpoint, "%s/%s/_zdbhighlighter", indexDescriptor->url, indexDescriptor->alias != NULL ? indexDescriptor->alias : indexDescriptor->fullyQualifiedName);
    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);

    freeStringInfo(endpoint);
    freeStringInfo(request);
    return response->data;
}

void elasticsearch_freeSearchResponse(ZDBSearchResponse *searchResponse) {
    freeStringInfo(searchResponse->httpResponse);
    pfree(searchResponse);
}

static uint64 count_deleted_docs(ZDBIndexDescriptor *indexDescriptor) {
	StringInfo endpoint = makeStringInfo();
	StringInfo response;

	appendStringInfo(endpoint, "%s/_cat/indices/%s?h=docs.deleted", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
	response = rest_call("GET", endpoint->data, NULL, indexDescriptor->compressionLevel);

	return (uint64) atoll(response->data);
}

void elasticsearch_bulkDelete(ZDBIndexDescriptor *indexDescriptor, List *itemPointers, bool isdeleted) {
	StringInfo endpoint = makeStringInfo();
	StringInfo request  = makeStringInfo();
	StringInfo response;
    ListCell *lc;

    if (isdeleted)
        appendStringInfo(endpoint, "%s/%s/_bulk?consistency=default", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    else
        appendStringInfo(endpoint, "%s/%s/data/_zdbbulk?consistency=default", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    if (strcmp("-1", indexDescriptor->refreshInterval) == 0) {
        appendStringInfo(endpoint, "&refresh=true");
    }

    foreach (lc, itemPointers) {
        ItemPointer item = lfirst(lc);

        appendStringInfo(request, "{\"delete\":{\"_type\":\"data\",\"_id\":\"%d-%d\"}}\n", ItemPointerGetBlockNumber(item), ItemPointerGetOffsetNumber(item));
        if (isdeleted)
            appendStringInfo(request, "{\"delete\":{\"_type\":\"deleted\",\"_id\":\"%d-%d\"}}\n", ItemPointerGetBlockNumber(item), ItemPointerGetOffsetNumber(item));

        if (request->len >= indexDescriptor->batch_size) {
            response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);
            checkForBulkError(response, "delete");

            resetStringInfo(request);
            freeStringInfo(response);
        }
    }

    if (request->len > 0) {
        response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);
        checkForBulkError(response, "delete");
    }

	if (indexDescriptor->optimizeAfter > 0) {
		uint64 deleted_docs = count_deleted_docs(indexDescriptor);

		if (deleted_docs >=  indexDescriptor->optimizeAfter) {
			resetStringInfo(endpoint);
			appendStringInfo(endpoint, "%s/%s/_optimize?only_expunge_deletes=true", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

			elog(ZDB_LOG_LEVEL, "[zombodb vacuum] expunging deleted docs in %s (docs.deleted=%lu)", indexDescriptor->fullyQualifiedName, deleted_docs);
			rest_call("GET", endpoint->data, NULL, indexDescriptor->compressionLevel);
		}
	}

    freeStringInfo(endpoint);
    freeStringInfo(request);
}

char *elasticsearch_vacuumSupport(ZDBIndexDescriptor *indexDescriptor, char *type) {
    StringInfo endpoint = makeStringInfo();
    StringInfo response;
    Snapshot snapshot = GetActiveSnapshot();

    appendStringInfo(endpoint, "%s/%s/_zdbvacuum?type=%s&xmin=%lu&xmax=%lu", indexDescriptor->url, indexDescriptor->fullyQualifiedName, type, convert_xid(snapshot->xmin), convert_xid(snapshot->xmax));
    response = rest_call("GET", endpoint->data, NULL, indexDescriptor->compressionLevel);

    freeStringInfo(endpoint);
	if (response->len > 0 && response->data[0] == '{' && strstr(response->data, "error") != NULL)
		elog(ERROR, "%s", response->data);
    return response->data;
}

static void appendBatchInsertData(ZDBIndexDescriptor *indexDescriptor, ItemPointer ht_ctid, text *value, StringInfo bulk, bool isupdate, ItemPointer old_ctid, TransactionId xmin, uint64 sequence) {
    /* the data */
    appendStringInfo(bulk, "{\"index\":{\"_id\":\"%d-%d\"}}\n", ItemPointerGetBlockNumber(ht_ctid), ItemPointerGetOffsetNumber(ht_ctid));

    if (indexDescriptor->hasJson)
        appendBinaryStringInfoAndStripLineBreaks(bulk, VARDATA(value), VARSIZE(value) - VARHDRSZ);
    else
        appendBinaryStringInfo(bulk, VARDATA(value), VARSIZE(value) - VARHDRSZ);

    /* backup to remove the last '}' of the value json, so that we can... */
    while (bulk->data[bulk->len] != '}')
        bulk->len--;

    /* ...append our transaction id to the json */
    appendStringInfo(bulk, ",\"_xid\":%lu", convert_xid(xmin));

	/* and the sequence number */
	appendStringInfo(bulk, ",\"_zdb_seq\":%lu", sequence);

	if (isupdate)
		appendStringInfo(bulk, ",\"_prev_ctid\":\"%d-%d\"", ItemPointerGetBlockNumber(old_ctid), ItemPointerGetOffsetNumber(old_ctid));

	appendStringInfo(bulk, "}\n");
}

static PostDataEntry *checkout_batch_pool(BatchInsertData *batch) {
    int i;

    for (i=0; i<batch->rest->nhandles+1; i++) {
        if (batch->pool[i] != NULL) {
            PostDataEntry *entry = palloc(sizeof(PostDataEntry));

            entry->buff     = batch->pool[i];
            entry->pool_idx = i;

            batch->pool[i] = NULL;
            return entry;
        }
    }

    elog(ERROR, "Unable to checkout from batch pool");
}

void
elasticsearch_batchInsertRow(ZDBIndexDescriptor *indexDescriptor, ItemPointer ctid, text *data, bool isupdate, ItemPointer old_ctid, TransactionId xid, CommandId commandId, uint64 sequence) {
    BatchInsertData *batch = lookup_batch_insert_data(indexDescriptor, true);
    bool fast_path = false;

    if (batch->bulk == NULL)
        batch->bulk = checkout_batch_pool(batch);

    appendBatchInsertData(indexDescriptor, ctid, data, batch->bulk->buff, isupdate, old_ctid, xid, sequence);
    batch->nprocessed++;
    batch->nrecs++;

    if (rest_multi_perform(batch->rest)) {
        int before = batch->rest->available;

        rest_multi_partial_cleanup(batch->rest, false, true);
        fast_path = batch->rest->available > before && batch->nrecs >= (batch->nprocessed/batch->nrequests) - 250;
    }

    if (fast_path || batch->bulk->buff->len >= indexDescriptor->batch_size) {
        StringInfo endpoint = makeStringInfo();

        /* don't &refresh=true here as a full .refreshIndex() is called after batchInsertFinish() */
        appendStringInfo(endpoint, "%s/%s/data/_zdbbulk?consistency=default", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

        /* send the request to index this batch */
        rest_multi_call(batch->rest, "POST", endpoint->data, batch->bulk, indexDescriptor->compressionLevel);
        elog(ZDB_LOG_LEVEL, "[zombodb] Indexed %d rows for %s (%d in batch, active=%d)", batch->nprocessed, indexDescriptor->fullyQualifiedName, batch->nrecs, batch->rest->nhandles - batch->rest->available);

        /* reset the bulk StringInfo for the next batch of records */
        batch->bulk  = checkout_batch_pool(batch);
        batch->nrecs = 0;
        batch->nrequests++;
    }
}

void elasticsearch_batchInsertFinish(ZDBIndexDescriptor *indexDescriptor) {
    BatchInsertData *batch = lookup_batch_insert_data(indexDescriptor, false);

    if (batch) {
        if (batch->nrequests > 0) {
            /** wait for all outstanding HTTP requests to finish */
            while (!rest_multi_all_done(batch->rest)) {
                rest_multi_is_available(batch->rest);
                CHECK_FOR_INTERRUPTS();
            }
            rest_multi_partial_cleanup(batch->rest, true, false);
        }

        if (batch->bulk->buff->len > 0) {
            StringInfo endpoint = makeStringInfo();
            StringInfo response;

            appendStringInfo(endpoint, "%s/%s/data/_zdbbulk?consistency=default", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

            if (batch->nrequests == 0) {
				/*
				 * We need to refresh only if we're in a transaction block.
				 *
				 * If we're not, the index will be refreshed when the transaction commits
				 */
				if (IsTransactionBlock()) {
					/*
					 * if this is the only request being made in this batch, then we'll &refresh=true
					 * to avoid an additional round-trip to ES, but only if a) we're not in batch mode
					 * and b) if the index refresh interval is -1
					 */
					if (!zdb_batch_mode_guc) {
						if (strcmp("-1", indexDescriptor->refreshInterval) == 0) {
							appendStringInfo(endpoint, "&refresh=true");
						}
					}
				}

                response = rest_call("POST", endpoint->data, batch->bulk->buff, indexDescriptor->compressionLevel);
            } else {
                /*
                 * otherwise we'll do a full refresh below, so there's no need to do it here
                 */
                response = rest_call("POST", endpoint->data, batch->bulk->buff, indexDescriptor->compressionLevel);
            }
            checkForBulkError(response, "batch finish");
            freeStringInfo(response);
        }

        if (batch->nrequests > 0) {
            elog(ZDB_LOG_LEVEL, "[zombodb] Indexed %d rows in %d requests for %s", batch->nprocessed,
                 batch->nrequests + 1, indexDescriptor->fullyQualifiedName);

			/*
			 * We need to refresh only if we're in a transaction block.
			 *
			 * If we're not, the index will be refreshed when the transaction commits
			 */
			if (IsTransactionBlock()) {
				/*
				 * If this wasn't the only request being made in this batch
				 * then ask ES to refresh the index, but only if a) we're not in batch mode
				 * and b) if the index refresh interval is -1
				 */
				elasticsearch_refreshIndex(indexDescriptor);
			}
        }

        batchInsertDataList = list_delete(batchInsertDataList, batch);
        pfree(batch);
    }
}

void elasticsearch_deleteTuples(ZDBIndexDescriptor *indexDescriptor, List *ctids) {
    StringInfo endpoint = makeStringInfo();
    StringInfo request = makeStringInfo();
    StringInfo response;
    ListCell *lc;
    uint64 xid = convert_xid(GetCurrentTransactionId());

    appendStringInfo(endpoint, "%s/%s/deleted/_bulk", indexDescriptor->url, indexDescriptor->fullyQualifiedName);
    if (strcmp("-1", indexDescriptor->refreshInterval) == 0) {
        appendStringInfo(endpoint, "?refresh=true");
    }

    foreach (lc, ctids) {
        ItemPointer ctid = (ItemPointer) lfirst(lc);

        appendStringInfo(request, "{\"index\":{\"_id\":\"%d-%d\"}}\n", ItemPointerGetBlockNumber(ctid), ItemPointerGetOffsetNumber(ctid));
        appendStringInfo(request, "{\"_deleting_xid\":%lu}\n", xid);
    }

    response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);
    checkForBulkError(response, "delete tuples");
}

void elasticsearch_markTransactionCommitted(ZDBIndexDescriptor *indexDescriptor, TransactionId xid) {
	uint64 convertedXid = convert_xid(xid);
	StringInfo endpoint = makeStringInfo();
	StringInfo request  = makeStringInfo();
	StringInfo response;

	appendStringInfo(request, "%lu", convertedXid);
	appendStringInfo(endpoint, "%s/%s/_zdbxid", indexDescriptor->url, indexDescriptor->fullyQualifiedName);

    /* we always want to refresh the index so long as the user hasn't specified a refresh interval */
    if (strcmp("-1", indexDescriptor->refreshInterval) == 0) {
        appendStringInfo(endpoint, "?refresh=true");
    }

	response = rest_call("POST", endpoint->data, request, indexDescriptor->compressionLevel);
	checkForBulkError(response, "mark transaction committed");
}

void elasticsearch_transactionFinish(ZDBIndexDescriptor *indexDescriptor, ZDBTransactionCompletionType completionType) {
    batchInsertDataList = NULL;
}
