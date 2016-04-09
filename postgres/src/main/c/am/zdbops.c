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
#include "executor/spi.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
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
PG_FUNCTION_INFO_V1(zdb_internal_get_index_field_lists);
PG_FUNCTION_INFO_V1(zdb_internal_highlight);
PG_FUNCTION_INFO_V1(zdb_internal_multi_search);
PG_FUNCTION_INFO_V1(zdb_internal_analyze_text);


/*
 * taken from Postgres' rewriteHandler.c
 *
 * NB:  This function is exposed in PG 9.4+
 */
static Query *get_view_query(Relation view) {
    int i;

    Assert(view->rd_rel->relkind == RELKIND_VIEW);

    for (i = 0; i < view->rd_rules->numLocks; i++) {
        RewriteRule *rule = view->rd_rules->rules[i];

        if (rule->event == CMD_SELECT) {
            /* A _RETURN rule should have only one action */
            if (list_length(rule->actions) != 1)
                elog(ERROR, "invalid _RETURN rule action specification");

            return (Query *) linitial(rule->actions);
        }
    }

    elog(ERROR, "failed to find _RETURN rule for view");
    return NULL;                /* keep compiler quiet */
}

static FuncExpr *extract_zdb_funcExpr_from_view(Relation viewRel, Oid *heapRelOid) {
    ListCell *lc;
    Query    *viewDef;
    FuncExpr *funcExpr;

    viewDef = get_view_query(viewRel);

    foreach(lc, viewDef->targetList) {
        TargetEntry *te = (TargetEntry *) lfirst(lc);

        if (te->resname && strcmp("zdb", te->resname) == 0) {
            if (!IsA(te->expr, FuncExpr))
                elog(ERROR, "The 'zdb' column in view '%s' is not a function", RelationGetRelationName(viewRel));

            funcExpr = (FuncExpr *) te->expr;

            validate_zdb_funcExpr(funcExpr, heapRelOid);

            return funcExpr;
        }
    }

    elog(ERROR, "No column named 'zdb' in view '%s'", RelationGetRelationName(viewRel));
    return NULL;
}

void validate_zdb_funcExpr(FuncExpr *funcExpr, Oid *heapRelOid) {
    Node *a1, *a2;

    if (list_length(funcExpr->args) != 2)
        elog(ERROR, "Incorrect number of arguments to the 'zdb' column function");

    a1 = linitial(funcExpr->args);
    a2 = lsecond(funcExpr->args);

    if (!IsA(a1, Const) || ((Const *) a1)->consttype != REGCLASSOID)
        elog(ERROR, "First argument of the 'zdb' column function is not ::regclass");
    else if (!IsA(a2, Var) || ((Var *) a2)->vartype != TIDOID)
        elog(ERROR, "Second argument of the 'zdb' column function is not ::tid");

    *heapRelOid = (Oid) DatumGetObjectId(((Const *) a1)->constvalue);
}

Oid zdb_determine_index_oid(FuncExpr *funcExpr, Oid heapRelOid) {
    /**
     * The index we use here is the index from the table specified by the column named 'zdb'
     * that has a functional expression that matches the functional expression of the column named 'zdb'
     */
    Oid      zdbIndexRelId = InvalidOid;
    Relation heapRel;
    List     *indexes;
    ListCell *lc;

    heapRel = RelationIdGetRelation(heapRelOid);
    indexes = RelationGetIndexList(heapRel);
    foreach (lc, indexes) {
        Oid      indexRelOid = (Oid) lfirst_oid(lc);
        Relation indexRel;
        NameData amname;

        indexRel = RelationIdGetRelation(indexRelOid);
        amname   = indexRel->rd_am->amname;
        RelationClose(indexRel);

        if (strcmp("zombodb", amname.data) == 0) {
            Node *n = linitial(RelationGetIndexExpressions(indexRel));

            if (IsA(n, FuncExpr) && ((FuncExpr *) n)->funcid == funcExpr->funcid) {
                zdbIndexRelId = indexRelOid;
                break;
            }
        }
    }

    if (zdbIndexRelId == InvalidOid) {
        RelationClose(heapRel);
        elog(ERROR, "Unable to find ZomboDB index for table '%s'", RelationGetRelationName(heapRel));
    }

    RelationClose(heapRel);
    return zdbIndexRelId;
}

Datum zdb_determine_index(PG_FUNCTION_ARGS) {
    Oid      relid         = PG_GETARG_OID(0);
    Oid      zdbIndexRelId = InvalidOid;
    Relation table_or_view_rel;

    table_or_view_rel = RelationIdGetRelation(relid);

    switch (table_or_view_rel->rd_rel->relkind) {
        case RELKIND_VIEW: {
            Oid      heapRelOid;
            FuncExpr *funcExpr;

            funcExpr      = extract_zdb_funcExpr_from_view(table_or_view_rel, &heapRelOid);
            zdbIndexRelId = zdb_determine_index_oid(funcExpr, heapRelOid);

            break;
        }

        case RELKIND_MATVIEW:
        case RELKIND_RELATION: {
            /*
             * the index to use is the first non-shadow "zombodb" index we find on the relation.
             * there should only be one
             */
            List     *indexes = RelationGetIndexList(table_or_view_rel);
            ListCell *lc;

            foreach(lc, indexes) {
                Oid      indexRelOid = (Oid) lfirst_oid(lc);
                Relation indexRel    = RelationIdGetRelation(indexRelOid);

                if (strcmp("zombodb", indexRel->rd_am->amname.data) == 0 && ZDBIndexOptionsGetShadow(indexRel) == NULL)
                    zdbIndexRelId = indexRelOid;

                RelationClose(indexRel);

                if (zdbIndexRelId != InvalidOid)
                    break;
            }
            break;
        }

        default:
            elog(ERROR, "'%s' is an unsupported kind of %c", RelationGetRelationName(table_or_view_rel), table_or_view_rel->rd_rel->relkind);
            break;
    }

    RelationClose(table_or_view_rel);

    if (zdbIndexRelId == InvalidOid)
        elog(ERROR, "Cannot determine which ZomboDB index to use for '%s'", RelationGetRelationName(table_or_view_rel));

    PG_RETURN_OID(zdbIndexRelId);
}

Datum zdb_get_index_name(PG_FUNCTION_ARGS) {
    Oid                index_oid = PG_GETARG_OID(0);
    Relation           indexRel;
    ZDBIndexDescriptor *desc;

    indexRel = RelationIdGetRelation(index_oid);
    desc     = zdb_alloc_index_descriptor(indexRel);
    RelationClose(indexRel);

    if (desc->fullyQualifiedName == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(cstring_to_text(desc->fullyQualifiedName));
}

Datum zdb_get_url(PG_FUNCTION_ARGS) {
    Oid                index_oid = PG_GETARG_OID(0);
    Relation           indexRel;
    ZDBIndexDescriptor *desc;

    indexRel = RelationIdGetRelation(index_oid);
    desc     = zdb_alloc_index_descriptor(indexRel);
    RelationClose(indexRel);

    if (desc->url == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(cstring_to_text(desc->url));
}

Datum zdb_query_func(PG_FUNCTION_ARGS) {
    elog(ERROR, "operator '==>(json, text)' not supported");
    PG_RETURN_BOOL(false);
}

Datum zdb_tid_query_func(PG_FUNCTION_ARGS) {
    return zdb_seqscan(fcinfo);
}

Datum zdb_table_ref_and_tid(PG_FUNCTION_ARGS) {
    PG_RETURN_POINTER(PG_GETARG_POINTER(1));
}

Datum zdb_row_to_json(PG_FUNCTION_ARGS) {
    Datum row = PG_GETARG_DATUM(0);

    PG_RETURN_DATUM(DirectFunctionCall1(row_to_json, row));
}

Datum zdb_internal_describe_nested_object(PG_FUNCTION_ARGS) {
    Oid                indexoid   = PG_GETARG_OID(0);
    char               *fieldname = GET_STR(PG_GETARG_TEXT_P(1));
    ZDBIndexDescriptor *desc;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexoid);

    PG_RETURN_TEXT_P(CStringGetTextDatum(desc->implementation->describeNestedObject(desc, fieldname)));
}

Datum zdb_internal_get_index_mapping(PG_FUNCTION_ARGS) {
    Oid                indexoid = PG_GETARG_OID(0);
    ZDBIndexDescriptor *desc;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexoid);

    PG_RETURN_TEXT_P(CStringGetTextDatum(desc->implementation->getIndexMapping(desc)));
}

Datum zdb_internal_get_index_field_lists(PG_FUNCTION_ARGS) {
    Oid                index_oid = PG_GETARG_OID(0);
    Relation           indexRel;
    ZDBIndexDescriptor *desc;

    indexRel = RelationIdGetRelation(index_oid);
    desc     = zdb_alloc_index_descriptor(indexRel);
    RelationClose(indexRel);

    if (desc->fieldLists == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(cstring_to_text(desc->fieldLists));
}

Datum zdb_internal_highlight(PG_FUNCTION_ARGS) {
    Oid                indexoid      = PG_GETARG_OID(0);
    char               *query        = GET_STR(PG_GETARG_TEXT_P(1));
    char               *documentJson = GET_STR(PG_GETARG_TEXT_P(2));
    ZDBIndexDescriptor *desc;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexoid);

    PG_RETURN_TEXT_P(CStringGetTextDatum(desc->implementation->highlight(desc, query, documentJson)));
}

Datum zdb_internal_multi_search(PG_FUNCTION_ARGS) {
    ArrayType *oidArray   = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *queryArray = PG_GETARG_ARRAYTYPE_P(1);
    Oid       *oids;
    char      **queries;
    int       oid_many, query_many;
    char      *response;

    oids    = oid_array_to_oids(oidArray, &oid_many);
    queries = text_array_to_strings(queryArray, &query_many);

    if (oid_many != query_many)
        elog(ERROR, "Number of indexes and queries must match.  %d != %d", oid_many, query_many);
    else if (oid_many == 0)
        PG_RETURN_NULL();

    response = zdb_multi_search(oids, queries, oid_many);
    if (!response)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(CStringGetTextDatum(response));
}

Datum zdb_internal_analyze_text(PG_FUNCTION_ARGS) {
    Oid                indexRel       = PG_GETARG_OID(0);
    char               *analyzer_name = GET_STR(PG_GETARG_TEXT_P(1));
    char               *data          = GET_STR(PG_GETARG_TEXT_P(2));
    ZDBIndexDescriptor *desc;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexRel);

    PG_RETURN_TEXT_P(CStringGetTextDatum(desc->implementation->analyzeText(desc, analyzer_name, data)));
}

Datum make_es_mapping(Oid tableRelId, TupleDesc tupdesc, bool isAnonymous) {
    StringInfo result = makeStringInfo();
    char       *json;
    int        i, cnt = 0;

    appendStringInfo(result, "{\"is_anonymous\": %s,", isAnonymous ? "true" : "false");
    appendStringInfo(result, "\"properties\": {");

    for (i = 0; i < tupdesc->natts; i++) {
        char *name;
        char *typename;
        char *user_mapping;

        if (tupdesc->attrs[i]->attisdropped)
            continue;

        name = NameStr(tupdesc->attrs[i]->attname);

        if (cnt > 0) appendStringInfoCharMacro(result, ',');

        /* if we have a user-defined mapping for this field in this table, use it */
        user_mapping = lookup_field_mapping(CurrentMemoryContext, tableRelId, name);
        if (user_mapping != NULL) {
            appendStringInfo(result, "\"%s\": %s", name, user_mapping);
            continue;
        }

        /* otherwise, build a mapping based on the field type */
        typename = DatumGetCString(DirectFunctionCall1(regtypeout, Int32GetDatum(tupdesc->attrs[i]->atttypid)));
        appendStringInfo(result, "\"%s\": {", name);
        appendStringInfo(result, "\"store\":false,");

        if (strcmp("fulltext", typename) == 0) {
            /* phrase-indexed field */
            appendStringInfo(result, "\"type\": \"string\",");
            appendStringInfo(result, "\"index_options\": \"positions\",");
            appendStringInfo(result, "\"include_in_all\": \"false\",");
            appendStringInfo(result, "\"analyzer\": \"fulltext\",");
            appendStringInfo(result, "\"fielddata\": { \"format\": \"disabled\" },");
            appendStringInfo(result, "\"norms\": {\"enabled\":true}");

        } else if (strcmp("fulltext_with_shingles", typename) == 0) {
            /* phrase-indexed field */
            appendStringInfo(result, "\"type\": \"string\",");
            appendStringInfo(result, "\"index_options\": \"positions\",");
            appendStringInfo(result, "\"include_in_all\": \"false\",");
            appendStringInfo(result, "\"index_analyzer\": \"fulltext_with_shingles\",");
            appendStringInfo(result, "\"search_analyzer\": \"fulltext_with_shingles_search\",");
            appendStringInfo(result, "\"fielddata\": { \"format\": \"disabled\" },");
            appendStringInfo(result, "\"norms\": {\"enabled\":true}");

        } else if (strcmp("phrase", typename) == 0 || strcmp("phrase_array", typename) == 0) {
            /* phrase-indexed field */
            appendStringInfo(result, "\"type\": \"string\",");
            appendStringInfo(result, "\"index_options\": \"positions\",");
            appendStringInfo(result, "\"analyzer\": \"phrase\",");
            appendStringInfo(result, "\"fielddata\": { \"format\": \"paged_bytes\" },");
            appendStringInfo(result, "\"norms\": {\"enabled\":true}");

        } else if (strcmp("date", typename) == 0 || strcmp("date[]", typename) == 0) {
            /* date field */
            appendStringInfo(result, "\"type\": \"string\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"index\": \"not_analyzed\",");
            appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
            appendStringInfo(result, "\"fields\": {"
                    "   \"date\" : {\"type\" : \"date\", \"index\" : \"not_analyzed\"}"
                    "}");

        } else if (strcmp("timestamp", typename) == 0 || strcmp("timestamp without time zone", typename) == 0 ||
                   strcmp("timestamp[]", typename) == 0 || strcmp("timestamp without time zone[]", typename) == 0) {
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

        } else if (strcmp("timestamp with time zone", typename) == 0 ||
                   strcmp("timestamp with time zone[]", typename) == 0) {
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

        } else if (strcmp("time", typename) == 0 || strcmp("time[]", typename) == 0 ||
                   strcmp("time without time zone", typename) == 0 ||
                   strcmp("time without time zone[]", typename) == 0) {
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

        } else if (strcmp("time with time zone", typename) == 0 || strcmp("time with time zone[]", typename) == 0) {
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

        } else if (strcmp("smallint", typename) == 0 || strcmp("integer", typename) == 0 ||
                   strcmp("smallint[]", typename) == 0 || strcmp("integer[]", typename) == 0) {
            /* integer field */
            appendStringInfo(result, "\"type\": \"integer\",");
            appendStringInfo(result, "\"store\": \"true\",");
            appendStringInfo(result, "\"include_in_all\": \"false\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
            appendStringInfo(result, "\"index\": \"not_analyzed\"");

        } else if (strcmp("bigint", typename) == 0 || strcmp("numeric", typename) == 0 ||
                   strcmp("bigint[]", typename) == 0 || strcmp("numeric[]", typename) == 0) {
            /* long field */
            appendStringInfo(result, "\"type\": \"long\",");
            appendStringInfo(result, "\"store\": \"true\",");
            appendStringInfo(result, "\"include_in_all\": \"false\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
            appendStringInfo(result, "\"index\": \"not_analyzed\"");

        } else if (strcmp("float", typename) == 0 || strcmp("float[]", typename) == 0) {
            /* float field */
            appendStringInfo(result, "\"type\": \"float\",");
            appendStringInfo(result, "\"include_in_all\": \"false\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
            appendStringInfo(result, "\"index\": \"not_analyzed\"");

        } else if (strcmp("double precision", typename) == 0 || strcmp("double precision[]", typename) == 0) {
            /* double field */
            appendStringInfo(result, "\"type\": \"double\",");
            appendStringInfo(result, "\"include_in_all\": \"false\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
            appendStringInfo(result, "\"index\": \"not_analyzed\"");

        } else if (strcmp("boolean", typename) == 0 || strcmp("boolean[]", typename) == 0) {
            /* boolean field */
            appendStringInfo(result, "\"type\": \"boolean\",");
            appendStringInfo(result, "\"include_in_all\": \"false\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"fielddata\": {\"format\": \"doc_values\"},");
            appendStringInfo(result, "\"index\": \"not_analyzed\"");

        } else if (strcmp("text", typename) == 0 || strcmp("varchar", typename) == 0 ||
                   strcmp("character", typename) == 0 || strcmp("character varying", typename) == 0 ||
                   strcmp("text[]", typename) == 0 || strcmp("varchar[]", typename) == 0 ||
                   strcmp("character[]", typename) == 0 || strcmp("character varying[]", typename) == 0 ||
                   strcmp("uuid", typename) == 0 || strcmp("uuid[]", typename) == 0) {
            /* string field */
            appendStringInfo(result, "\"type\": \"string\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"index_options\": \"docs\",");
            appendStringInfo(result, "\"ignore_above\":32000,");
            appendStringInfo(result, "\"analyzer\": \"exact\"");

        } else if (strcmp("json", typename) == 0) {
            /* json field */
            appendStringInfo(result, "\"type\": \"nested\",");
            appendStringInfo(result, "\"norms\": {\"enabled\":false},");
            appendStringInfo(result, "\"include_in_parent\":true,");
            appendStringInfo(result, "\"include_in_root\":true,");
            appendStringInfo(result, "\"ignore_above\":32000,");
            appendStringInfo(result, "\"include_in_all\":true");

        } else {
            Oid base_type;

            if (type_is_domain(typename, &base_type)) {
                /*
                 * The type is a domain, so we want to treat this as an analyzed string
                 * where the type name is the actual analyzer name
                 */
                char *analyzer = typename;

                switch (base_type) {
                    case TEXTOID:
                        /**
                         * If the underlying type is 'text', then we want to
                         * treat this as if it were of type 'fulltext', in that
                         * it's NOT included in _all and fielddata is disabled
                         */
                        appendStringInfo(result, "\"type\": \"string\",");
                        appendStringInfo(result, "\"index_options\": \"positions\",");
                        appendStringInfo(result, "\"include_in_all\": \"false\",");
                        appendStringInfo(result, "\"analyzer\": \"%s\",", analyzer);
                        appendStringInfo(result, "\"fielddata\": { \"format\": \"disabled\" },");
                        appendStringInfo(result, "\"ignore_above\":32000,");
                        appendStringInfo(result, "\"norms\": {\"enabled\":true}");
                        break;

                    case VARCHAROID:
                        /**
                         * If the underlying type is 'varchar' (of any length), then we
                         * want to treat this as if it were of type 'phrase' in that
                         * it *is* included in _all and fielddata is enabled
                         */
                        appendStringInfo(result, "\"type\": \"string\",");
                        appendStringInfo(result, "\"index_options\": \"positions\",");
                        appendStringInfo(result, "\"include_in_all\": \"true\",");
                        appendStringInfo(result, "\"analyzer\": \"%s\",", analyzer);
                        appendStringInfo(result, "\"fielddata\": { \"format\": \"paged_bytes\" },");
                        appendStringInfo(result, "\"ignore_above\":32000,");
                        appendStringInfo(result, "\"norms\": {\"enabled\":true}");
                        break;

                    default:
                        /* Otherwise we just can't handle it */
                        elog(ERROR, "Don't know how to generate a mapping for the domain %s", typename);
                }
            } else {
                /* we're unsure about this type, so pretend it's an 'exact' analyzed string */
                appendStringInfo(result, "\"type\": \"string\",");
                appendStringInfo(result, "\"norms\": {\"enabled\":false},");
                appendStringInfo(result, "\"index_options\": \"docs\",");
                appendStringInfo(result, "\"ignore_above\":32000,");
                appendStringInfo(result, "\"analyzer\": \"exact\"");

                /* warn about it so users know what's happening */
                elog(WARNING, "Unrecognized data type %s, pretending it's of type 'text'", typename);
            }
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