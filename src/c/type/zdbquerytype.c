/**
 * Copyright 2018 ZomboDB, LLC
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

#include "zombodb.h"

#include "elasticsearch/querygen.h"
#include "json/json_support.h"

#include "utils/jsonb.h"
#include "utils/json.h"
#include "libpq/pqformat.h"

PG_FUNCTION_INFO_V1(zdbquery_in);
PG_FUNCTION_INFO_V1(zdbquery_out);
PG_FUNCTION_INFO_V1(zdbquery_recv);
PG_FUNCTION_INFO_V1(zdbquery_send);

PG_FUNCTION_INFO_V1(zdbquery_from_text);
PG_FUNCTION_INFO_V1(zdbquery_from_jsonb);
PG_FUNCTION_INFO_V1(zdbquery_to_json);
PG_FUNCTION_INFO_V1(zdbquery_to_jsonb);

static bool zdbquery_string_is_zdb(char *query) {
	if (!is_json(query))
		return false;

	void *json = parse_json_object_from_string(query, CurrentMemoryContext);
	bool is_zdb = get_json_object_object(json, "query_dsl", true) != NULL;

	pfree(json);
	return is_zdb;
}

static bool zdbquery_has_no_options(ZDBQueryType *query) {
	return zdbquery_get_limmit(query) == 0 &&
		   zdbquery_get_offset(query) == 0 &&
		   zdbquery_get_maxscore(query) == 0.0 &&
		   zdbquery_get_row_estimate(query) == zdb_default_row_estimation_guc &&
		   zdbquery_get_sort_field(query) == NULL &&
		   zdbquery_get_sort_direction(query) == NULL;
}

static char *zdbquery_to_minimal_json(ZDBQueryType *query) {
	/*
	 * for brevity, especially in EXPLAIN output, output only the actual ES query if the query
	 * has no other ZDB-specific options
	 */
	if (zdbquery_has_no_options(query))
		return zdbquery_get_query(query);
	else
		return query->json;
}

ZDBQueryType *zdbquery_in_direct(char *input) {
	size_t       len;
	ZDBQueryType *result;
	char *json;

	if (zdbquery_string_is_zdb(input)) {
		/* it's already in the format we expect */
		json = input;
	} else {
		/* we need to build it ourself */
		json = psprintf("{\"query_dsl\":%s}", convert_to_query_dsl_not_wrapped(input));
	}

	len = strlen(json);
	result = palloc0(sizeof(ZDBQueryType) + len + 1);

	memcpy(result->json, json, len + 1);
	SET_VARSIZE(result, sizeof(ZDBQueryType) + len + 1);
	return result;
}

Datum zdbquery_in(PG_FUNCTION_ARGS) {
	return PointerGetDatum(zdbquery_in_direct(PG_GETARG_CSTRING(0)));
}

Datum zdbquery_out(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	char *queryJson = zdbquery_to_minimal_json(query);
	void *json = parse_json_object_from_string(queryJson, CurrentMemoryContext);
	void *queryStringClause = get_json_object_object(json, "query_string", true);

	if (queryStringClause != NULL) {
		/* peek inside the "query_string" clause and pluck out its "query" */
		char *queryValue = pstrdup((char *) get_json_object_string(queryStringClause, "query", false));

		pfree(json);
		PG_RETURN_CSTRING(queryValue);
	}

	pfree(json);
	PG_RETURN_CSTRING(pstrdup(queryJson));
}

Datum zdbquery_recv(PG_FUNCTION_ARGS) {
	StringInfo   msg = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_POINTER(zdbquery_in_direct((char *) pq_getmsgstring(msg)));
}

Datum zdbquery_send(PG_FUNCTION_ARGS) {
	ZDBQueryType   *zdbquery = (ZDBQueryType *) PG_GETARG_POINTER(0);
	StringInfoData msg;

	pq_begintypsend(&msg);
	pq_sendstring(&msg, zdbquery->json);
	PG_RETURN_BYTEA_P(pq_endtypsend(&msg));
}

Datum zdbquery_from_text(PG_FUNCTION_ARGS) {
	char         *input = GET_STR(PG_GETARG_TEXT_P(0));
	PG_RETURN_POINTER(zdbquery_in_direct(input));
}

Datum zdbquery_from_jsonb(PG_FUNCTION_ARGS) {
	Jsonb *json = PG_GETARG_JSONB(0);
	Datum json_as_text;

	json_as_text = CStringGetTextDatum(DatumGetCString(DirectFunctionCall1(jsonb_out, JsonbGetDatum(json))));

	return DirectFunctionCall1(zdbquery_from_text, json_as_text);
}

Datum zdbquery_to_json(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	PG_RETURN_DATUM(DirectFunctionCall1(json_in, CStringGetDatum(zdbquery_to_minimal_json(query))));
}

Datum zdbquery_to_jsonb(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(zdbquery_to_minimal_json(query))));
}

int zdbquery_get_row_estimate(ZDBQueryType *query) {
	void *json = parse_json_object_from_string(query->json, CurrentMemoryContext);
	int estimate = (int) get_json_object_uint64(json, "row_estimate", true);

	if (estimate == 0)
		estimate = (int) get_json_object_uint64(json, "limit", true);

	if (estimate == 0)
		estimate = zdb_default_row_estimation_guc;

	pfree(json);
	return estimate;
}

double zdbquery_get_maxscore(ZDBQueryType *query) {
	void *json = parse_json_object_from_string(query->json, CurrentMemoryContext);
	double maxscore = get_json_object_real(json, "maxscore");

	pfree(json);
	return maxscore;
}

int zdbquery_get_limmit(ZDBQueryType *query) {
	void *json = parse_json_object_from_string(query->json, CurrentMemoryContext);
	int estimate = (int) get_json_object_uint64(json, "limit", true);

	pfree(json);
	return estimate;
}

int zdbquery_get_offset(ZDBQueryType *query) {
	void *json = parse_json_object_from_string(query->json, CurrentMemoryContext);
	int estimate = (int) get_json_object_uint64(json, "offset", true);

	pfree(json);
	return estimate;
}

char *zdbquery_get_sort_field(ZDBQueryType *query) {
	void *json = parse_json_object_from_string(query->json, CurrentMemoryContext);
	char *sortField = (char *) get_json_object_string(json, "sort_field", true);

	if (sortField != NULL)
		sortField = pstrdup(sortField);

	pfree(json);
	return sortField;
}

char *zdbquery_get_sort_direction(ZDBQueryType *query) {
	void *json = parse_json_object_from_string(query->json, CurrentMemoryContext);
	char *sortDirection = (char *) get_json_object_string(json, "sort_direction", true);

	if (sortDirection != NULL)
		sortDirection = pstrdup(sortDirection);

	pfree(json);
	return sortDirection;
}

char *zdbquery_get_query(ZDBQueryType *query) {
	void *json = parse_json_object_from_string(query->json, CurrentMemoryContext);
	void *queryObject = (char *) get_json_object_object(json, "query_dsl", true);
	char *queryString = write_json(queryObject);

	if (queryString != NULL)
		queryString = pstrdup(queryString);

	pfree(json);
	return queryString;
}
