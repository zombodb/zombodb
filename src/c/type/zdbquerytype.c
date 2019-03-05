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

PG_FUNCTION_INFO_V1(zdb_set_query_property);

static bool zdbquery_string_is_zdb(char *query) {
	void *json;
	bool is_zdb;

	if (!is_json(query))
		return false;

	json = parse_json_object_from_string(query, CurrentMemoryContext);
	is_zdb = get_json_object_object(json, "query_dsl", true) != NULL;

	pfree(json);
	return is_zdb;
}

static bool zdbquery_has_no_options(ZDBQueryType *query) {
	return zdbquery_get_limit(query) == 0 &&
		   zdbquery_get_offset(query) == 0 &&
		   zdbquery_get_min_score(query) == 0.0 &&
		   zdbquery_get_row_estimate(query) == zdb_default_row_estimation_guc &&
		   zdbquery_get_sort_json(query) == NULL &&
		   zdbquery_get_wants_score(query) == false;
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
	char         *json;

	if (zdbquery_string_is_zdb(input)) {
		/* it's already in the format we expect */
		json = input;
	} else {
		/* we need to build it ourself */
		json = psprintf("{\"query_dsl\":%s}", convert_to_query_dsl_not_wrapped(input));
	}

	len    = strlen(json);
	result = palloc0(sizeof(ZDBQueryType) + len + 1);

	memcpy(result->json, json, len + 1);
	SET_VARSIZE(result, sizeof(ZDBQueryType) + len + 1);
	return result;
}

Datum zdbquery_in(PG_FUNCTION_ARGS) {
	return PointerGetDatum(zdbquery_in_direct(PG_GETARG_CSTRING(0)));
}

Datum zdbquery_out(PG_FUNCTION_ARGS) {
	ZDBQueryType *query             = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	char         *queryJson         = zdbquery_to_minimal_json(query);
	void         *json              = parse_json_object_from_string(queryJson, CurrentMemoryContext);
	void         *queryStringClause = get_json_object_object(json, "query_string", true);

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
	StringInfo msg = (StringInfo) PG_GETARG_POINTER(0);

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
	char *input = GET_STR(PG_GETARG_TEXT_P(0));
	PG_RETURN_POINTER(zdbquery_in_direct(input));
}

Datum zdbquery_from_jsonb(PG_FUNCTION_ARGS) {
	Jsonb *json = PG_GETARG_JSONB_P(0);
	Datum json_as_text;

	json_as_text = CStringGetTextDatum(DatumGetCString(DirectFunctionCall1(jsonb_out, JsonbPGetDatum(json))));

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

static uint64 zdbquery_get_raw_row_estimate(ZDBQueryType *query) {
	void   *json    = parse_json_object_from_string(query->json, CurrentMemoryContext);
	uint64 estimate = get_json_object_uint64(json, "row_estimate", true);

	pfree(json);
	return estimate;
}

bool zdbquery_get_wants_score(ZDBQueryType *query) {
	void   *json    = parse_json_object_from_string(query->json, CurrentMemoryContext);
	bool wants_score = get_json_object_bool(json, "wants_score", true);

	pfree(json);
	return wants_score;
}

uint64 zdbquery_get_row_estimate(ZDBQueryType *query) {
	void   *json    = parse_json_object_from_string(query->json, CurrentMemoryContext);
	uint64 estimate = get_json_object_uint64(json, "row_estimate", true);

	if (estimate == 0)
		estimate = get_json_object_uint64(json, "limit", true);

	if (estimate == 0)
		estimate = (uint64) zdb_default_row_estimation_guc;

	pfree(json);
	return estimate;
}

double zdbquery_get_min_score(ZDBQueryType *query) {
	void   *json     = parse_json_object_from_string(query->json, CurrentMemoryContext);
	float8 min_score = get_json_object_real(json, "min_score");

	pfree(json);
	return min_score;
}

uint64 zdbquery_get_limit(ZDBQueryType *query) {
	void   *json    = parse_json_object_from_string(query->json, CurrentMemoryContext);
	uint64 estimate = get_json_object_uint64(json, "limit", true);

	pfree(json);
	return estimate;
}

uint64 zdbquery_get_offset(ZDBQueryType *query) {
	void   *json    = parse_json_object_from_string(query->json, CurrentMemoryContext);
	uint64 estimate = get_json_object_uint64(json, "offset", true);

	pfree(json);
	return estimate;
}

char *zdbquery_get_sort_json(ZDBQueryType *query) {
	void *json        = parse_json_object_from_string(query->json, CurrentMemoryContext);
	void *sortObject = (char *) get_json_object_object(json, "sort_json", true);
	char *sortString = write_json(sortObject);

	if (sortString != NULL)
		sortString = pstrdup(sortString);

	pfree(json);
	return sortString;
}

char *zdbquery_get_query(ZDBQueryType *query) {
	void *json        = parse_json_object_from_string(query->json, CurrentMemoryContext);
	void *queryObject = (char *) get_json_object_object(json, "query_dsl", true);
	char *queryString = write_json(queryObject);

	if (queryString != NULL)
		queryString = pstrdup(queryString);

	pfree(json);
	return queryString;
}

#define ZDBQUERY_MAX_KEYS 7
typedef enum zdbquery_properties {
	zdbquery_limit = 0,
	zdbquery_offset,
	zdbquery_min_score,
	zdbquery_sort_json,
	zdbquery_row_estimate,
	zdbquery_query_dsl,
	zdbquery_wants_score
} zdbquery_properties;

static zdbquery_properties zdbquery_key_to_propenum(char *key) {
	if (strcmp(key, "limit") == 0) {
		return zdbquery_limit;
	} else if (strcmp(key, "offset") == 0) {
		return zdbquery_offset;
	} else if (strcmp(key, "min_score") == 0) {
		return zdbquery_min_score;
	} else if (strcmp(key, "sort_json") == 0) {
		return zdbquery_sort_json;
	} else if (strcmp(key, "row_estimate") == 0) {
		return zdbquery_row_estimate;
	} else if (strcmp(key, "query_dsl") == 0) {
		return zdbquery_query_dsl;
	} else if (strcmp(key, "wants_score") == 0) {
		return zdbquery_wants_score;
	}

	elog(ERROR, "unrecognized zdbquery property: %s", key);
}

Datum zdb_set_query_property(PG_FUNCTION_ARGS) {
	char                *key   = GET_STR(PG_GETARG_TEXT_P(0));
	char                *value = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBQueryType        *input = (ZDBQueryType *) PG_GETARG_VARLENA_P(2);
	zdbquery_properties prop   = zdbquery_key_to_propenum(key);
	StringInfo          query  = makeStringInfo();
	int                 i;

	appendStringInfoChar(query, '{');
	for (i = 0; i < ZDBQUERY_MAX_KEYS; i++) {

		switch (i) {
			case zdbquery_limit: {
				uint64 limit = prop == i ? DatumGetUInt64(DirectFunctionCall1(int8in, CStringGetDatum(value)))
										 : zdbquery_get_limit(
								input);

				if (limit > 0) {
					if (query->len > 1)
						appendStringInfoChar(query, ',');
					appendStringInfo(query, "\"limit\":%lu", limit);
				}
			}
				break;

			case zdbquery_offset: {
				uint64 offset = prop == i ? DatumGetUInt64(DirectFunctionCall1(int8in, CStringGetDatum(value)))
										  : zdbquery_get_offset(input);

				if (offset > 0) {
					if (query->len > 1)
						appendStringInfoChar(query, ',');
					appendStringInfo(query, "\"offset\":%lu", offset);
				}
			}
				break;

			case zdbquery_min_score: {
				float8 min_score = prop == i ? DatumGetFloat8(DirectFunctionCall1(float8in, CStringGetDatum(value)))
											 : zdbquery_get_min_score(input);

				if (min_score > 0.0) {
					if (query->len > 1)
						appendStringInfoChar(query, ',');
					appendStringInfo(query, "\"min_score\":%f", min_score);
				}
			}
				break;

			case zdbquery_sort_json: {
				char *sort_json = prop == i ? value : zdbquery_get_sort_json(input);

				if (sort_json != NULL) {
					if (query->len > 1)
						appendStringInfoChar(query, ',');

					appendStringInfo(query, "\"sort_json\":%s", sort_json);
				}
			}
				break;

			case zdbquery_row_estimate: {
				uint64 row_estimate = prop == i ? DatumGetUInt64(DirectFunctionCall1(int8in, CStringGetDatum(value)))
												: zdbquery_get_raw_row_estimate(input);

				if (row_estimate > 0) {
					if (query->len > 1)
						appendStringInfoChar(query, ',');
					appendStringInfo(query, "\"row_estimate\":%lu", row_estimate);
				}
			}
				break;

			case zdbquery_query_dsl: {
				char *query_dsl = prop == i ? value : zdbquery_get_query(input);

				if (query_dsl != NULL) {
					if (query->len > 1)
						appendStringInfoChar(query, ',');
					appendStringInfo(query, "\"query_dsl\":%s", query_dsl);
				}
			}
				break;

			case zdbquery_wants_score: {
				bool wants_score = prop == i ? DatumGetBool(DirectFunctionCall1(boolin, CStringGetDatum(value)))
											 : zdbquery_get_wants_score(input);

				if (wants_score) {
					if (query->len > 1)
						appendStringInfoChar(query, ',');
					appendStringInfo(query, "\"wants_score\":true");
				}
			}
				break;

			default:
				elog(ERROR, "unexpected property index: %d", i);
		}
	}
	appendStringInfoChar(query, '}');

	PG_RETURN_POINTER(MakeZDBQuery(query->data));
}
