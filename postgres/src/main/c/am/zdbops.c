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
#include "executor/spi.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/rel.h"

#include "zdb_interface.h"
#include "zdbops.h"
#include "zdbseqscan.h"
#include "util/zdbutils.h"

PG_FUNCTION_INFO_V1(zdb_determine_index);
PG_FUNCTION_INFO_V1(zdb_get_index_name);
PG_FUNCTION_INFO_V1(zdb_get_url);
PG_FUNCTION_INFO_V1(zdb_query_func);
PG_FUNCTION_INFO_V1(zdb_tid_query_func);
PG_FUNCTION_INFO_V1(zdb_table_ref_and_tid);
PG_FUNCTION_INFO_V1(zdb_row_to_json);
PG_FUNCTION_INFO_V1(zdb_internal_describe_nested_object);
PG_FUNCTION_INFO_V1(zdb_internal_get_index_mapping);
PG_FUNCTION_INFO_V1(zdb_internal_highlight);

Datum zdb_determine_index(PG_FUNCTION_ARGS) {
	Oid      relid         = PG_GETARG_OID(0);
	Oid      zdbIndexRelId = InvalidOid;
	Relation table_or_view_rel;

	table_or_view_rel = RelationIdGetRelation(relid);

	switch (table_or_view_rel->rd_rel->relkind) {
		case RELKIND_VIEW: {
			/**
			 * The index we use here is the index from the table specified by the column named 'zdb'
			 * that has a functional expression that matches the functional expression of the column named 'zdb'
			 */
			TupleDesc tupdesc = RelationGetDescr(table_or_view_rel);
			int       i;

			for (i = 0; i < tupdesc->natts; i++) {
				Form_pg_attribute att = tupdesc->attrs[i];

				if (att->attisdropped)
					continue;

				if (strcmp("zdb", att->attname.data) == 0) {
					StringInfo pg_rewriteQuery = makeStringInfo();
					ListCell   *lc;
					char       *action;
					Query      *query;

					/* figure out the view definition */
					appendStringInfo(pg_rewriteQuery, "SELECT ev_action "
							"FROM pg_catalog.pg_rewrite "
							"WHERE rulename = '_RETURN' AND ev_class=%d", relid);

					SPI_connect();
					if (SPI_exec(pg_rewriteQuery->data, 2) != SPI_OK_SELECT)
						elog(ERROR, "failed to get pg_rewrite tuple for view '%s'", RelationGetRelationName(table_or_view_rel));
					else if (SPI_processed != 1)
						elog(ERROR, "'%s' not a view", RelationGetRelationName(table_or_view_rel));

					action = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

					if (action == NULL)
						elog(ERROR, "pg_rewrite.ev_action IS NULL for view '%s'", RelationGetRelationName(table_or_view_rel));

					/* and turn it into a Query so we can walk its target list */
					query = (Query *) linitial(stringToNode(action));

					foreach(lc, query->targetList) {
						TargetEntry *te = (TargetEntry *) lfirst(lc);

						if (strcmp("zdb", te->resname) == 0 && IsA(te->expr, FuncExpr)) {
							FuncExpr *viewFuncExpr = (FuncExpr *) te->expr;
							Const    *tableRegclass;
							Oid      heapRelOid;
							Relation heapRel;
							List     *indexes;
							ListCell *lc2;

							if (list_length(viewFuncExpr->args) != 2)
								elog(ERROR, "the 'zdb' column for view '%s' does not have the correct number of arguments (2)", RelationGetRelationName(table_or_view_rel));
							else if (!IsA(linitial(viewFuncExpr->args), Const))
								elog(ERROR, "the 'zdb' column's function for view '%s' shoud have a regclass constant as the first argument", RelationGetRelationName(table_or_view_rel));

							tableRegclass = (Const *) linitial(viewFuncExpr->args);
							heapRelOid    = (Oid) DatumGetObjectId(tableRegclass->constvalue);

							heapRel = RelationIdGetRelation(heapRelOid);
							indexes = RelationGetIndexList(heapRel);
							foreach (lc2, indexes) {
								Oid      indexRelOid = (Oid) lfirst(lc2);
								Relation indexRel;

								indexRel = RelationIdGetRelation(indexRelOid);
								if (strcmp("zombodb", indexRel->rd_am->amname.data) == 0) {
									ListCell *lc3;

									foreach (lc3, RelationGetIndexExpressions(indexRel)) {
										Node *n = (Node *) lfirst(lc3);

										if (IsA(n, FuncExpr)) {
											FuncExpr *indexFuncExpr = (FuncExpr *) n;

											if (indexFuncExpr->funcid == viewFuncExpr->funcid) {
												zdbIndexRelId = indexRelOid;
												break;
											}
										}
									}
								}

								RelationClose(indexRel);

								if (zdbIndexRelId != InvalidOid)
									break;
							}

							RelationClose(heapRel);
						}

						if (zdbIndexRelId != InvalidOid)
							break;
					}
					SPI_finish();

					if (zdbIndexRelId != InvalidOid)
						break;
				}
			}

			if (zdbIndexRelId != InvalidOid)
				break;

			elog(ERROR, "view '%s' does not have a column named 'zdb' defined as a functional expression", RelationGetRelationName(table_or_view_rel));
			break;
		}

		case RELKIND_MATVIEW:
		case RELKIND_RELATION: {
			/*
			 * the index to use is the first non-shadow "zombodb" index we find on the relation
			 * there should only be one
			 */
			List     *indexes = RelationGetIndexList(table_or_view_rel);
			ListCell *lc;

			foreach(lc, indexes) {
				Oid      indexRelOid = (Oid) lfirst(lc);
				Relation indexRel    = RelationIdGetRelation(indexRelOid);

				if (strcmp("zombodb", indexRel->rd_am->amname.data) == 0 && ZDBIndexOptionsGetShadow(indexRel) == NULL)
					zdbIndexRelId = indexRelOid;

				RelationClose(indexRel);

				if (zdbIndexRelId != InvalidOid)
					break;
			}
			break;
		}

		case RELKIND_COMPOSITE_TYPE:
		case RELKIND_FOREIGN_TABLE:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		default:
			elog(ERROR, "cannot support relkind of %c", table_or_view_rel->rd_rel->relkind);
			break;
	}

	RelationClose(table_or_view_rel);

	PG_RETURN_OID(zdbIndexRelId);
}

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
	elog(ERROR, "operator '==>(json, text)' not supported");
	PG_RETURN_BOOL(false);
}

Datum zdb_tid_query_func(PG_FUNCTION_ARGS)
{
	return zdb_seqscan(fcinfo);
}

Datum zdb_table_ref_and_tid(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(PG_GETARG_POINTER(1));
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
				strcmp("text[]", typename) == 0 || strcmp("varchar[]", typename) == 0 || strcmp("character[]", typename) == 0 || strcmp("character varying[]", typename) == 0 ||
				strcmp("uuid", typename) == 0 || strcmp("uuid[]", typename) == 0)
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
			appendStringInfo(result, "\"include_in_parent\":true,");
			appendStringInfo(result, "\"include_in_root\":true,");
			appendStringInfo(result, "\"include_in_all\":true");

		}
		else
		{
			/* unrecognized type, so just treat as an 'exact' field */
			appendStringInfo(result, "\"type\": \"string\",");
			appendStringInfo(result, "\"norms\": {\"enabled\":false},");
			appendStringInfo(result, "\"index_options\": \"docs\",");
			appendStringInfo(result, "\"analyzer\": \"exact\"");

			/* but we do want to warn about it so users know what's happening */
			elog(WARNING, "Unrecognized data type %s, pretending it's of type 'text'", typename);
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

