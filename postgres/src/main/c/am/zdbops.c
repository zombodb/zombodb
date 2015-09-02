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
#include "postgres.h"
#include "executor/spi.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/json.h"

#include "zdb_interface.h"
#include "zdbops.h"
#include "util/zdbutils.h"

PG_FUNCTION_INFO_V1(zdb_get_index_name);
PG_FUNCTION_INFO_V1(zdb_get_url);
PG_FUNCTION_INFO_V1(zdb_query_func);
PG_FUNCTION_INFO_V1(zdb_row_to_json);
PG_FUNCTION_INFO_V1(zdb_internal_describe_nested_object);
PG_FUNCTION_INFO_V1(zdb_internal_get_index_mapping);
PG_FUNCTION_INFO_V1(zdb_internal_highlight);

Datum zdb_get_index_name(PG_FUNCTION_ARGS)
{
	Oid index_oid = PG_GETARG_OID(0);
	Relation indexRel;
	ZDBIndexDescriptor *desc;

	indexRel = RelationIdGetRelation(index_oid);
	desc = zdb_alloc_index_descriptor(indexRel);
	RelationClose(indexRel);

	if (desc->fullyQualifiedName == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(desc->fullyQualifiedName));
}

Datum zdb_get_url(PG_FUNCTION_ARGS)
{
	Oid index_oid = PG_GETARG_OID(0);
	Relation indexRel;
	ZDBIndexDescriptor *desc;

	indexRel = RelationIdGetRelation(index_oid);
	desc = zdb_alloc_index_descriptor(indexRel);
	RelationClose(indexRel);

	if (desc->url == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(desc->url));
}

Datum zdb_query_func(PG_FUNCTION_ARGS)
{
//	Datum left = PG_GETARG_DATUM(0);
//	char *right = GET_STR(PG_GETARG_TEXT_P(1));

	elog(ERROR, "zdb_query_func: not implemented");
	PG_RETURN_BOOL(false);
}

Datum zdb_row_to_json(PG_FUNCTION_ARGS)
{
	Datum           row = PG_GETARG_DATUM(0);

	PG_RETURN_DATUM(DirectFunctionCall1(row_to_json, row));
}

Datum zdb_internal_describe_nested_object(PG_FUNCTION_ARGS)
{
	Oid indexoid = PG_GETARG_OID(0);
	char *fieldname = GET_STR(PG_GETARG_TEXT_P(1));
	ZDBIndexDescriptor *desc;

	desc = zdb_alloc_index_descriptor_by_index_oid(indexoid);

	PG_RETURN_TEXT_P(CStringGetTextDatum(desc->implementation->describeNestedObject(desc, fieldname)));
}

Datum zdb_internal_get_index_mapping(PG_FUNCTION_ARGS)
{
	Oid indexoid = PG_GETARG_OID(0);
	ZDBIndexDescriptor *desc;

	desc = zdb_alloc_index_descriptor_by_index_oid(indexoid);

	PG_RETURN_TEXT_P(CStringGetTextDatum(desc->implementation->getIndexMapping(desc)));
}

Datum zdb_internal_highlight(PG_FUNCTION_ARGS)
{
	Oid indexoid = PG_GETARG_OID(0);
	char *query = GET_STR(PG_GETARG_TEXT_P(1));
	char *documentJson = GET_STR(PG_GETARG_TEXT_P(2));
	ZDBIndexDescriptor *desc;

	desc = zdb_alloc_index_descriptor_by_index_oid(indexoid);

	PG_RETURN_TEXT_P(CStringGetTextDatum(desc->implementation->highlight(desc, query, documentJson)));
}

Datum make_es_mapping(TupleDesc tupdesc, bool isAnonymous)
{
	StringInfo result = makeStringInfo();
	char       *json;
	int        i, cnt = 0;

	appendStringInfo(result, "{\"is_anonymous\": %s,", isAnonymous ? "true" : "false");
	appendStringInfo(result, "\"properties\": {");

	for (i = 0; i < tupdesc->natts; i++)
	{
		char *name;
		char *typename;

		if (tupdesc->attrs[i]->attisdropped)
			continue;

        name = NameStr(tupdesc->attrs[i]->attname);
		typename = DatumGetCString(DirectFunctionCall1(regtypeout, Int32GetDatum(tupdesc->attrs[i]->atttypid)));

		if (cnt > 0) appendStringInfoCharMacro(result, ',');
		appendStringInfo(result, "\"%s\": {", name);

		appendStringInfo(result, "\"store\":false,");

		if (strcmp("fulltext", typename) == 0)
		{
			/* phrase-indexed field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"index_options\": \"positions\",");
			appendStringInfo(result, "\"include_in_all\": \"false\",");
			appendStringInfo(result, "\"analyzer\": \"fulltext\",");
			appendStringInfo(result, "\"fielddata\": { \"format\": \"disabled\" },");
			appendStringInfo(result, "\"norms\": {\"enabled\":false}");

		}
		else if (strcmp("phrase", typename) == 0 || strcmp("phrase_array", typename) == 0)
		{
			/* phrase-indexed field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"index_options\": \"positions\",");
			appendStringInfo(result, "\"analyzer\": \"phrase\",");
			appendStringInfo(result, "\"fielddata\": { \"format\": \"paged_bytes\" },");
			appendStringInfo(result, "\"norms\": {\"enabled\":false}");

		}
		else if (strcmp("date", typename) == 0 || strcmp("date[]", typename) == 0)
		{
			/* date field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\",");
			appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
			appendStringInfo(result, "\"fields\": {"
					                 "   \"date\" : {\"type\" : \"date\", \"index\" : \"not_analyzed\"}"
					                 "}");

		}
		else if (strcmp("timestamp", typename) == 0  || strcmp("timestamp without time zone", typename) == 0 ||
				strcmp("timestamp[]", typename) == 0 || strcmp("timestamp without time zone[]", typename) == 0)
		{
			/* timestamp field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\",");
			appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
			appendStringInfo(result, "\"fields\": {"
					                 "   \"date\" : {\"type\" : \"date\", \"index\" : \"not_analyzed\", "
					                 "               \"format\": \"yyyy-MM-dd HH:mm:ss.SSSSSSSSSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSSSSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSSSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SSS||"
					                                              "yyyy-MM-dd HH:mm:ss.SS||"
					                                              "yyyy-MM-dd HH:mm:ss.S||"
					                                              "yyyy-MM-dd HH:mm:ss\"}"
					                 "}");

		}
		else if (strcmp("timestamp with time zone", typename) == 0 || strcmp("timestamp with time zone[]", typename) == 0)
		{
			/* timestamp field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\",");
			appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
			appendStringInfo(result, "\"fields\": {"
					                 "   \"date\" : {\"type\" : \"date\", \"index\" : \"not_analyzed\", "
					                 "               \"format\": \"yyyy-MM-dd HH:mm:ss.SSSSSSSSSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSSSSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSSSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SSZ||"
					                                              "yyyy-MM-dd HH:mm:ss.SZ||"
					                                              "yyyy-MM-dd HH:mm:ssZ\"}"
					                 "}");

		}
		else if (strcmp("time", typename) == 0 || strcmp("time[]", typename) == 0 || strcmp("time without time zone", typename) == 0 || strcmp("time without time zone[]", typename) == 0)
		{
			/* time field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\",");
			appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
			appendStringInfo(result, "\"fields\": {"
					                 "   \"date\" : {\"type\" : \"date\", \"index\" : \"not_analyzed\", "
					                 "               \"format\": \"HH:mm:ss.SSSSSSSSSS||"
					                                              "HH:mm:ss.SSSSSSSSS||"
					                                              "HH:mm:ss.SSSSSSSS||"
					                                              "HH:mm:ss.SSSSSSS||"
					                                              "HH:mm:ss.SSSSSS||"
					                                              "HH:mm:ss.SSSSS||"
					                                              "HH:mm:ss.SSSS||"
					                                              "HH:mm:ss.SSS||"
					                                              "HH:mm:ss.SS||"
					                                              "HH:mm:ss.S||"
					                                              "HH:mm:ss\"}"
					                 "}");

		}
		else if (strcmp("time with time zone", typename) == 0 || strcmp("time with time zone[]", typename) == 0)
		{
			/* time field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\",");
			appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
			appendStringInfo(result, "\"fields\": {"
					                 "   \"date\" : {\"type\" : \"date\", \"index\" : \"not_analyzed\", "
					                 "               \"format\": \"HH:mm:ss.SSSSSSSSSSZ||"
					                                              "HH:mm:ss.SSSSSSSSSZ||"
					                                              "HH:mm:ss.SSSSSSSSZ||"
					                                              "HH:mm:ss.SSSSSSSZ||"
					                                              "HH:mm:ss.SSSSSSZ||"
					                                              "HH:mm:ss.SSSSSZ||"
					                                              "HH:mm:ss.SSSSZ||"
					                                              "HH:mm:ss.SSSZ||"
					                                              "HH:mm:ss.SSZ||"
					                                              "HH:mm:ss.SZ||"
					                                              "HH:mm:ssZ\"}"
					                 "}");

		}
		else if (strcmp("smallint", typename) == 0 || strcmp("integer", typename) == 0 ||
				strcmp("smallint[]", typename) == 0 || strcmp("integer[]", typename) == 0)
		{
			/* integer field */
			appendStringInfo(result, "\"type\": \"integer\",");
			appendStringInfo(result, "\"store\": \"true\",");
			appendStringInfo(result, "\"include_in_all\": \"false\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\"");

		}
		else if (strcmp("bigint", typename) == 0 || strcmp("numeric", typename) == 0 ||
				strcmp("bigint[]", typename) == 0 || strcmp("numeric[]", typename) == 0)
		{
			/* long field */
			appendStringInfo(result, "\"type\": \"long\",");
			appendStringInfo(result, "\"store\": \"true\",");
			appendStringInfo(result, "\"include_in_all\": \"false\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\"");

		}
		else if (strcmp("float", typename) == 0 ||
				strcmp("float[]", typename) == 0)
		{
			/* float field */
			appendStringInfo(result, "\"type\": \"float\",");
			appendStringInfo(result, "\"include_in_all\": \"false\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\"");

		}
		else if (strcmp("double precision", typename) == 0 ||
				strcmp("double precision[]", typename) == 0)
		{
			/* double field */
			appendStringInfo(result, "\"type\": \"double\",");
			appendStringInfo(result, "\"include_in_all\": \"false\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\"");

		}
		else if (strcmp("boolean", typename) == 0 ||
				strcmp("boolean[]", typename) == 0)
		{
			/* boolean field */
			appendStringInfo(result, "\"type\": \"boolean\",");
			appendStringInfo(result, "\"include_in_all\": \"false\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index\": \"not_analyzed\"");

		}
		else if (strcmp("text", typename) == 0 || strcmp("varchar", typename) == 0 || strcmp("character", typename) == 0 || strcmp("character varying", typename) == 0 ||
				strcmp("text[]", typename) == 0 || strcmp("varchar[]", typename) == 0 || strcmp("character[]", typename) == 0 || strcmp("character varying[]", typename) == 0)
		{
			/* string field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index_options\": \"docs\",");
			appendStringInfo(result, "\"analyzer\": \"exact\"");

		}
		else if (strcmp("json", typename) == 0)
		{
			/* json field */
			appendStringInfo(result, "\"type\": \"nested\",");
			appendStringInfo(result, "\"include_in_all\": \"false\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"include_in_parent\":false,");
			appendStringInfo(result, "\"include_in_root\":false,");
			appendStringInfo(result, "\"include_in_all\":true");

		}
		else
		{
			elog(ERROR, "Unsupported type: %s", typename);
		}

		appendStringInfoCharMacro(result, '}');

		pfree(typename);
		cnt++;
	}
	appendStringInfo(result, "}}");

	json = result->data;
	pfree(result);

	return CStringGetTextDatum(json);
}

