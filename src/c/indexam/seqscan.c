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

#include "elasticsearch/elasticsearch.h"
#include "elasticsearch/querygen.h"
#include "highlighting/highlighting.h"
#include "scoring/scoring.h"

#include "parser/parsetree.h"

#define CMP_FUNC_ENTRY_KEYSIZE 8192

typedef struct CmpFuncKey {
	char key[CMP_FUNC_ENTRY_KEYSIZE];
} CmpFuncKey;

typedef struct CmpFuncEntry {
	/*lint -e754 ignore unused member */
	char key[CMP_FUNC_ENTRY_KEYSIZE];
	HTAB *hash;
} CmpFuncEntry;

PG_FUNCTION_INFO_V1(zdb_anyelement_cmpfunc_array_should);
PG_FUNCTION_INFO_V1(zdb_anyelement_cmpfunc_array_must);
PG_FUNCTION_INFO_V1(zdb_anyelement_cmpfunc_array_not);
PG_FUNCTION_INFO_V1(zdb_anyelement_cmpfunc);

PG_FUNCTION_INFO_V1(zdb_tid_cmpfunc_array_should);
PG_FUNCTION_INFO_V1(zdb_tid_cmpfunc_array_must);
PG_FUNCTION_INFO_V1(zdb_tid_cmpfunc_array_not);
PG_FUNCTION_INFO_V1(zdb_tid_cmpfunc);

extern List *currentQueryStack;

static float4 scoring_cb(ItemPointer ctid, void *arg) {
	HTAB          *hash = (HTAB *) arg;
	ZDBScoreKey   key;
	ZDBScoreEntry *entry;
	bool          found;

	assert(ctid != NULL);

	ItemPointerCopy(ctid, &key.ctid);
	entry = hash_search(hash, &key, HASH_FIND, &found);
	if (entry != NULL && found)
		return entry->score;

	return 0.0;
}

static List *highlight_cb(ItemPointer ctid, ZDBHighlightFieldnameData *field, void *arg) {
	HTAB              *hash = (HTAB *) arg;
	ZDBHighlightKey   key;
	ZDBHighlightEntry *entry;
	bool              found;

	assert(ctid != NULL);
	assert(field != NULL);

	memset(&key, 0, sizeof(ZDBHighlightKey));
	memcpy(&key.field.data, field->data, HIGHLIGHT_FIELD_MAX_LENGTH);
	ItemPointerCopy(ctid, &key.ctid);

	entry = hash_search(hash, &key, HASH_FIND, &found);
	if (entry != NULL && found)
		return entry->highlights;

	return NULL;
}

static HTAB *create_ctid_map(Relation heapRel, Relation indexRel, ZDBQueryType *query, MemoryContext memoryContext) {
	ElasticsearchScrollContext *scroll;
	HTAB                       *scoreHash     = scoring_create_lookup_table(memoryContext, "scores from seqscan");
	HTAB                       *highlightHash = highlight_create_lookup_table(memoryContext, "highlights from seqscan");

	scroll = ElasticsearchOpenScroll(indexRel, query, false, 0,
									 extract_highlight_info(NULL, RelationGetRelid(heapRel)), NULL, 0);

	scoring_register_callback(RelationGetRelid(heapRel), scoring_cb, scoreHash, memoryContext);
	highlight_register_callback(RelationGetRelid(heapRel), highlight_cb, highlightHash, memoryContext);

	while (scroll->cnt < scroll->total) {
		ZDBScoreKey     key;
		ZDBScoreEntry   *entry;
		bool            found;
		float4          score;
		zdb_json_object highlights;

		if (!ElasticsearchGetNextItemPointer(scroll, &key.ctid, NULL, &score, &highlights))
			break;

		entry = hash_search(scoreHash, &key, HASH_ENTER, &found);
		entry->score = score;

		save_highlights(highlightHash, &key.ctid, highlights);
	}

	ElasticsearchCloseScroll(scroll);

	return scoreHash;
}

static Datum do_cmpfunc(ItemPointer ctid, ZDBQueryType *userQuery, FmgrInfo *flinfo, Oid heapRelId) {
	QueryDesc     *currentQuery = linitial(currentQueryStack);
	HTAB          *hash         = (HTAB *) flinfo->fn_extra;
	bool          found;
	CmpFuncKey    key;
	CmpFuncEntry  *entry        = NULL;
	MemoryContext oldContext    = MemoryContextSwitchTo(currentQuery->estate->es_query_cxt);

	memset(&key, 0, sizeof(CmpFuncKey));
	snprintf(key.key, CMP_FUNC_ENTRY_KEYSIZE, "%s", zdbquery_get_query(userQuery));

	if (hash == NULL) {
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(HASHCTL));
		ctl.hash      = string_hash;
		ctl.keysize   = sizeof(CmpFuncKey);
		ctl.entrysize = sizeof(CmpFuncEntry);
		ctl.hcxt      = currentQuery->estate->es_query_cxt;    /* once this query is done, we don't care */

		hash = flinfo->fn_extra = hash_create("seqscan", 64, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
		entry       = hash_search(hash, &key, HASH_ENTER, &found);
		entry->hash = NULL;
	} else {
		entry = hash_search(hash, &key, HASH_FIND, &found);
		if (!found) {
			entry = hash_search(hash, &key, HASH_ENTER, &found);
			entry->hash = NULL;
		}
	}

	if (entry->hash == NULL) {
		/*
		 * execute query using our rhs argument and turn it into a hash
		 * and store that hash with this function for future evaluations
		 */
		Relation indexRel;
		Relation heapRel;

		heapRel  = relation_open(heapRelId, AccessShareLock);
		indexRel = find_zombodb_index(heapRel);
		entry->hash = create_ctid_map(heapRel, indexRel, userQuery, CurrentMemoryContext);
		relation_close(indexRel, AccessShareLock);
		relation_close(heapRel, AccessShareLock);
	}

	/* does our hash match the tuple currently being evaluated? */
	/*lint -e534 ignore return value */
	hash_search(entry->hash, ctid, HASH_FIND, &found);

	MemoryContextSwitchTo(oldContext);

	PG_RETURN_BOOL(found);
}

Datum zdb_anyelement_cmpfunc_array_should(PG_FUNCTION_ARGS) {
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("zombodb comparision function called in invalid context")));
}

Datum zdb_anyelement_cmpfunc_array_must(PG_FUNCTION_ARGS) {
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("zombodb comparision function called in invalid context")));
}

Datum zdb_anyelement_cmpfunc_array_not(PG_FUNCTION_ARGS) {
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("zombodb comparision function called in invalid context")));
}

Datum zdb_anyelement_cmpfunc(PG_FUNCTION_ARGS) {
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("zombodb comparision function called in invalid context")));
}

Datum zdb_tid_cmpfunc_array_should(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = array_to_should_query_dsl(DatumGetArrayTypeP(PG_GETARG_ARRAYTYPE_P(1)));
	return DirectFunctionCall2(zdb_tid_cmpfunc, PG_GETARG_DATUM(0), PointerGetDatum(query));
}

Datum zdb_tid_cmpfunc_array_must(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = array_to_must_query_dsl(DatumGetArrayTypeP(PG_GETARG_ARRAYTYPE_P(1)));
	return DirectFunctionCall2(zdb_tid_cmpfunc, PG_GETARG_DATUM(0), PointerGetDatum(query));
}

Datum zdb_tid_cmpfunc_array_not(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = array_to_not_query_dsl(DatumGetArrayTypeP(PG_GETARG_ARRAYTYPE_P(1)));
	return DirectFunctionCall2(zdb_tid_cmpfunc, PG_GETARG_DATUM(0), PointerGetDatum(query));
}

Datum zdb_tid_cmpfunc(PG_FUNCTION_ARGS) {
	ItemPointer   ctid   = (ItemPointer) PG_GETARG_POINTER(0);
	ZDBQueryType  *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(1);
	QueryDesc     *currentQuery;
	OpExpr        *opExpr;
	Node          *arg0;
	Var           *left;
	RangeTblEntry *rte;
	Index varno;

	if (!ItemPointerIsValid(ctid))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("Invalid ItemPointer passed to zombodb comparision function")));
	else if (!IsA(fcinfo->flinfo->fn_expr, OpExpr))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("zdb_tid_cmpfunc() not called as an operator expression")));
	else if (currentQueryStack == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("zdb_tid_cmpfunc() called in invalid context")));

	currentQuery = linitial(currentQueryStack);

	opExpr = (OpExpr *) fcinfo->flinfo->fn_expr;
	arg0   = linitial(opExpr->args);
	if (!IsA(arg0, Var))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("lhs of zdb_tid_cmpfunc() is not a Var")));
	left = (Var *) arg0;
	varno = left->varno;

	if (left->vartype != TIDOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("lhs of zdb_tid_cmpfunc() is not of type 'tid'")));

	if (varno == INNER_VAR || varno == OUTER_VAR) {
		varno = left->varnoold;
	}

	rte = rt_fetch(varno, currentQuery->plannedstmt->rtable);

	return do_cmpfunc(ctid, query, fcinfo->flinfo, rte->relid);
}
