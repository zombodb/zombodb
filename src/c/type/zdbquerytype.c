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

#include "utils/jsonb.h"
#include "libpq/pqformat.h"

PG_FUNCTION_INFO_V1(zdbquery_in);
PG_FUNCTION_INFO_V1(zdbquery_out);
PG_FUNCTION_INFO_V1(zdbquery_recv);
PG_FUNCTION_INFO_V1(zdbquery_send);

PG_FUNCTION_INFO_V1(zdbquery_from_text);
PG_FUNCTION_INFO_V1(zdbquery_from_jsonb);
PG_FUNCTION_INFO_V1(zdbquery_ctor);
PG_FUNCTION_INFO_V1(zdbquery_to_json);
PG_FUNCTION_INFO_V1(zdbquery_to_jsonb);

ZDBQueryType *zdbquery_in_direct(char *input) {
	size_t       len     = strlen(input);
	ZDBQueryType *result = palloc0(sizeof(ZDBQueryType) + len + 1);

	result->count_estimation = zdb_default_row_estimation_guc;
	memcpy(result->query_string, input, len + 1);
	SET_VARSIZE(result, sizeof(ZDBQueryType) + len + 1);
	return result;
}

Datum zdbquery_in(PG_FUNCTION_ARGS) {
	char         *input   = PG_GETARG_CSTRING(0);
	size_t       len      = strlen(input);
	StringInfo   estimate = makeStringInfo();
	ZDBQueryType *result;
	size_t       i;

	/* read the count estimation value */
	for (i = 0; i < len; i++) {
		char ch = input[i];

		if (ch == '-' || (ch >= '0' && ch <= '9')) {
			/* build the row estimate */
			appendStringInfoChar(estimate, ch);
		} else if (ch == ',') {
			/* skip the comma and get out -- we found the estimate */
			i++;
			break;
		} else if (ch == '{') {
			/* looks like the query is just json */
			i = 0;
			resetStringInfo(estimate);
			break;
		}
	}

	if (i >= len || estimate->len == 0) {
		/* no count estimate was provided so hardcode to 2500 */
		resetStringInfo(estimate);
		appendStringInfo(estimate, "%d", zdb_default_row_estimation_guc);
		i = 0;
	}

	/* subtract off the characters we consumed while reading the count estimate */
	len -= i;

	result = (ZDBQueryType *) palloc0(sizeof(ZDBQueryType) + len + 1);
	result->count_estimation = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(estimate->data)));
	memcpy(result->query_string, input + i, len + 1);
	SET_VARSIZE(result, sizeof(ZDBQueryType) + len + 1);

	PG_RETURN_POINTER(result);
}

Datum zdbquery_out(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	char         *result;

	/*
	 * for brevity, especially in EXPLAIN output, only output the count estimation if
	 * it's different than our default GUC setting
	 */
	if (query->count_estimation == zdb_default_row_estimation_guc)
		result = psprintf("%s", query->query_string);
	else
		result = psprintf("%d,%s", query->count_estimation, query->query_string);

	PG_RETURN_CSTRING(result);
}

Datum zdbquery_recv(PG_FUNCTION_ARGS) {
	StringInfo   buf = (StringInfo) PG_GETARG_POINTER(0);
	ZDBQueryType *result;
	int32        count_estimation;
	const char   *query;
	size_t       len;

	count_estimation = (int32) pq_getmsgint(buf, 4);
	query            = pq_getmsgstring(buf);
	len              = strlen(query);

	result = (ZDBQueryType *) palloc0(sizeof(ZDBQueryType) + len + 1);
	result->count_estimation = count_estimation;
	memcpy(result->query_string, query, len);
	SET_VARSIZE(result, sizeof(ZDBQueryType) + len + 1);

	PG_RETURN_POINTER(result);
}

Datum zdbquery_send(PG_FUNCTION_ARGS) {
	ZDBQueryType   *zdbquery = (ZDBQueryType *) PG_GETARG_POINTER(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, zdbquery->count_estimation, 4);
	pq_sendstring(&buf, zdbquery->query_string);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum zdbquery_from_text(PG_FUNCTION_ARGS) {
	char         *input = GET_STR(PG_GETARG_TEXT_P(0));
	size_t       len    = strlen(input);
	ZDBQueryType *zdbquery;

	zdbquery = palloc0(sizeof(ZDBQueryType) + len + 1);

	/* we assume, when casting from a text/json value, that we'll return 'zdb_default_row_estimation_guc' rows */
	zdbquery->count_estimation = zdb_default_row_estimation_guc;
	memcpy(zdbquery->query_string, input, len);
	SET_VARSIZE(zdbquery, sizeof(ZDBQueryType) + len + 1);

	PG_RETURN_POINTER(zdbquery);
}

Datum zdbquery_from_jsonb(PG_FUNCTION_ARGS) {
	Jsonb *json = PG_GETARG_JSONB(0);
	Datum json_as_text;

	json_as_text = CStringGetTextDatum(DatumGetCString(DirectFunctionCall1(jsonb_out, JsonbGetDatum(json))));

	return DirectFunctionCall1(zdbquery_from_text, json_as_text);
}

Datum zdbquery_ctor(PG_FUNCTION_ARGS) {
	int32        count_estimation = PG_GETARG_INT32(0);
	char         *query           = GET_STR(PG_GETARG_TEXT_P(1));
	size_t       len              = strlen(query);
	ZDBQueryType *zdbquery;

	zdbquery = palloc0(sizeof(ZDBQueryType) + len + 1);
	zdbquery->count_estimation = count_estimation;
	memcpy(zdbquery->query_string, query, len);
	SET_VARSIZE(zdbquery, sizeof(ZDBQueryType) + len + 1);

	PG_RETURN_POINTER(zdbquery);
}


Datum zdbquery_to_json(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	char         *dsl   = convert_to_query_dsl_not_wrapped(query->query_string);
	PG_RETURN_DATUM(DirectFunctionCall1(json_in, CStringGetDatum(dsl)));
}

Datum zdbquery_to_jsonb(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	char         *dsl   = convert_to_query_dsl_not_wrapped(query->query_string);
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(dsl)));
}
