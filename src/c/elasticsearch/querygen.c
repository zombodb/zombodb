/**
 * Copyright 2018-2020 ZomboDB, LLC
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

#include "querygen.h"

#include "json/json_support.h"

#include "utils/json.h"

/* from zdbfuncs.c */
extern Datum zdb_internal_visibility_clause(PG_FUNCTION_ARGS);

extern bool zdb_ignore_visibility_guc;

static char *wrap_with_visibility_query(Relation indexRel, char *query);

char *convert_to_query_dsl_not_wrapped(char *input) {
	/* trim away whitespace from the query */
	char *trimmed = TextDatumGetCString(
			DirectFunctionCall2(btrim, CStringGetTextDatum(input), CStringGetTextDatum(" \r\n\t\f")));

	if (strlen(trimmed) == 0) {
		/* it's zero-length, so that means to match everything */
		if (trimmed != input)
			pfree(trimmed);
		return pstrdup("{\"match_all\":{}}");
	} else if (is_json(trimmed)) {
		/* it's already JSON, so just return it as is */
		return trimmed;
	} else {
		/* convert the input text string into a "query_string" query */
		StringInfo query = makeStringInfo();
		char       *str;

		appendStringInfo(query, "{\"query_string\":{\"query\":");
		escape_json(query, trimmed);
		appendStringInfo(query, "}}");

		str = query->data;
		pfree(query);

		if (trimmed != input)
			pfree(trimmed);

		return str;
	}
}

static ZDBQueryType *do_scan_key(ScanKey key) {

	if (key->sk_flags & SK_SEARCHARRAY) {
		/* postgres wants us to search an array as a set of OR'd expressions */
		return array_to_should_query_dsl(DatumGetArrayTypeP(key->sk_argument));
	}

	switch (key->sk_strategy) {
		case ZDB_STRATEGY_ARRAY_SHOULD:
			return array_to_should_query_dsl(DatumGetArrayTypeP(key->sk_argument));
		case ZDB_STRATEGY_ARRAY_MUST:
			return array_to_must_query_dsl(DatumGetArrayTypeP(key->sk_argument));
		case ZDB_STRATEGY_ARRAY_NOT:
			return array_to_not_query_dsl(DatumGetArrayTypeP(key->sk_argument));
		case ZDB_STRATEGY_SINGLE: {
			ZDBQueryType *query = (ZDBQueryType *) DatumGetPointer(key->sk_argument);
			return MakeZDBQuery(convert_to_query_dsl_not_wrapped(zdbquery_get_query(query)));
		}
		default:
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Unrecognized ScanKey")));
}

static ZDBQueryType *to_bool_query(ArrayType *array, char *type) {
	StringInfo   should = makeStringInfo();
	int          many;
	int          i;
	ZDBQueryType **queries;

	queries = array_to_zdbqueries(array, &many);

	appendStringInfo(should, "{\"bool\":{\"%s\":[", type);
	for (i = 0; i < many; i++) {
		if (i > 0)
			appendStringInfoChar(should, ',');

		appendStringInfo(should, "%s", convert_to_query_dsl_not_wrapped(zdbquery_get_query(queries[i])));
	}
	appendStringInfo(should, "]}}");

	return MakeZDBQuery(should->data);
}

ZDBQueryType *array_to_should_query_dsl(ArrayType *array) {
	return to_bool_query(array, "should");
}

ZDBQueryType *array_to_must_query_dsl(ArrayType *array) {
	return to_bool_query(array, "must");
}

ZDBQueryType *array_to_not_query_dsl(ArrayType *array) {
	return to_bool_query(array, "must_not");
}

char *convert_to_query_dsl(Relation indexRel, ZDBQueryType *query, bool apply_visibility) {
	char *unwrapped;
	char *rc;

	unwrapped = convert_to_query_dsl_not_wrapped(zdbquery_get_query(query));
    if (zdb_ignore_visibility_guc) {
        /* session GUC says to apply no visibility whatsoever.  This is also important for VACUUM */
        rc = unwrapped;
    } else if (apply_visibility) {
        /* caller wants visibility -- likely from an aggregate function as normal SELECTs don't want visibility */
        rc = wrap_with_visibility_query(indexRel, unwrapped);
	} else {
		/* likely from a SELECT, so we need to make sure to filter out the _id:zdb_aborted_xids document */
		rc = psprintf("{\"bool\":{\"must\":[%s],\"must_not\":[{\"term\":{\"_id\":\"zdb_aborted_xids\"}}]}}", unwrapped);
	}

	if (unwrapped != rc)
		pfree(unwrapped);

	return rc;
}

ZDBQueryType *scan_keys_to_query_dsl(ScanKey keys, int nkeys) {
	bool hasArray = false;
	int  i;

	for (i = 0; i < nkeys; i++) {
		if (keys[i].sk_flags & SK_SEARCHARRAY || keys[i].sk_strategy == ZDB_STRATEGY_ARRAY_SHOULD ||
			keys[i].sk_strategy == ZDB_STRATEGY_ARRAY_MUST || keys[i].sk_strategy == ZDB_STRATEGY_ARRAY_NOT) {
			hasArray = true;
			break;
		}
	}

	if (nkeys == 1 && !hasArray) {
		return (ZDBQueryType *) (&keys[0])->sk_argument;
	} else {
		/*
		 * we have more than 1 ScanKey (or the one we have is an array), and one or more of them might be an array, so
		 * we need to convert them into an ES-compatible "bool" query where each key is a "must" clause, but array
		 * elements are in a sub-clause of "should"
		 */

		if (nkeys == 1) {
			/* just one key (that might be an array), so we can just convert it into its proper representation */
			return do_scan_key(&keys[0]);
		} else {
			/* multiple keys, so handle each one individually and put them in a "must" bool clause */
			StringInfo boolQuery = makeStringInfo();

			appendStringInfo(boolQuery, "{\"bool\":{\"must\":[");
			for (i = 0; i < nkeys; i++) {
				ScanKey      key    = &keys[i];
				ZDBQueryType *query = do_scan_key(key);

				if (i > 0)
					appendStringInfoChar(boolQuery, ',');
				appendStringInfo(boolQuery, "%s", convert_to_query_dsl_not_wrapped(zdbquery_get_query(query)));
				pfree(query);
			}
			appendStringInfo(boolQuery, "]}}");

			return MakeZDBQuery(boolQuery->data);
		}
	}
}

static char *wrap_with_visibility_query(Relation indexRel, char *query) {
    ZDBQueryType *vis = (ZDBQueryType *) DatumGetPointer(
            DirectFunctionCall1(zdb_internal_visibility_clause, ObjectIdGetDatum(RelationGetRelid(indexRel))));

    /* wrap the input query with the visibility query */
    return psprintf("{\"bool\":{\"must\":[%s],\"filter\":%s}}", query, zdbquery_get_query(vis));
}
