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

#include "elasticsearch/elasticsearch.h"
#include "elasticsearch/querygen.h"
#include "highlighting/highlighting.h"
#include "scoring/scoring.h"

#include "parser/parsetree.h"
#include "utils/lsyscache.h"

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

static List *highlight_cb(ItemPointer ctid, Name field, void *arg) {
	HTAB              *hash = (HTAB *) arg;
	ZDBHighlightKey   key;
	ZDBHighlightEntry *entry;
	bool              found;

	assert(ctid != NULL);
	assert(field != NULL);

	memset(&key, 0, sizeof(ZDBHighlightKey));
	memcpy(&key.field, field, NAMEDATALEN);
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

	scroll = ElasticsearchOpenScroll(indexRel, query, false, current_scan_wants_scores(NULL, heapRel), 0, extract_highlight_info(NULL, RelationGetRelid(heapRel)), NULL, 0);

	scoring_register_callback(RelationGetRelid(heapRel), scoring_cb, scoreHash, memoryContext);
	highlight_register_callback(RelationGetRelid(heapRel), highlight_cb, highlightHash, memoryContext);

	while (scroll->cnt < scroll->total) {
		ZDBScoreKey     key;
		ZDBScoreEntry   *entry;
		bool            found;
		float4          score;
		zdb_json_object highlights;

		ElasticsearchGetNextItemPointer(scroll, &key.ctid, NULL, &score, &highlights);

		entry = hash_search(scoreHash, &key, HASH_ENTER, &found);
		entry->score = score;

		save_highlights(highlightHash, &key.ctid, highlights);
	}

	ElasticsearchCloseScroll(scroll);

	return scoreHash;
}

static Datum do_cmpfunc(ZDBQueryType *userQuery, HTAB *cmpFuncHash, Oid typeoid, FunctionCallInfo fcinfo) {
	QueryDesc      *currentQuery;
	TupleTableSlot *slot = NULL;
	Oid            heapRelId;
	Relation       heapRel;
	ListCell       *lc;
	bool           found;

	if (currentQueryStack == NULL) {
		goto invalid_context_error;
	}

	currentQuery = linitial(currentQueryStack);

	heapRelId = get_typ_typrelid(typeoid);
	if (heapRelId == InvalidOid) {
		goto invalid_context_error;
	}

	/* find the TupleTableSlot that represents the row being evaluated in this scan */
	heapRel = RelationIdGetRelation(heapRelId);
	foreach(lc, currentQuery->estate->es_tupleTable) {
		TupleTableSlot *tmp = lfirst(lc);
		if (tmp->tts_tuple != NULL && tmp->tts_tuple->t_tableOid == RelationGetRelid(heapRel)) {
			slot = tmp;
			break;
		}
	}

	if (slot != NULL) {
		CmpFuncKey    key;
		CmpFuncEntry  *entry     = NULL;
		MemoryContext oldContext = MemoryContextSwitchTo(currentQuery->estate->es_query_cxt);

		memset(&key, 0, sizeof(CmpFuncKey));
		snprintf(key.key, CMP_FUNC_ENTRY_KEYSIZE, "%s", zdbquery_get_query(userQuery));

		if (cmpFuncHash == NULL) {
			HASHCTL ctl;

			memset(&ctl, 0, sizeof(HASHCTL));
			ctl.hash      = string_hash;
			ctl.keysize   = sizeof(CmpFuncKey);
			ctl.entrysize = sizeof(CmpFuncEntry);
			ctl.hcxt      = currentQuery->estate->es_query_cxt;    /* once this query is done, we don't care */

			cmpFuncHash = hash_create("seqscan", 64, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
			entry       = hash_search(cmpFuncHash, &key, HASH_ENTER, &found);
			entry->hash = NULL;

			fcinfo->flinfo->fn_extra = cmpFuncHash;
		} else {
			entry = hash_search(cmpFuncHash, &key, HASH_FIND, &found);
			if (!found) {
				entry = hash_search(cmpFuncHash, &key, HASH_ENTER, &found);
				entry->hash = NULL;
			}
		}

		if (entry->hash == NULL) {
			/*
			 * execute query using our rhs argument and turn it into a hash
			 * and store that hash with this function for future evaluations
			 */
			Relation indexRel;

			indexRel = find_index_relation(heapRel, typeoid, AccessShareLock);
			entry->hash = create_ctid_map(heapRel, indexRel, userQuery, currentQuery->estate->es_query_cxt);
			relation_close(indexRel, AccessShareLock);
		}

		/* does our hash match the tuple currently being evaluated? */
		/*lint -e534 ignore return value */
		hash_search(entry->hash, &slot->tts_tuple->t_self, HASH_FIND, &found);

		RelationClose(heapRel);
		MemoryContextSwitchTo(oldContext);

		PG_RETURN_BOOL(found);
	}

	invalid_context_error:
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("zombodb comparision function called in invalid context or lhs is not a table reference")));
	/*lint -e533 we either return a bool or elog(ERROR)*/
}

Datum zdb_anyelement_cmpfunc_array_should(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = array_to_should_query_dsl(DatumGetArrayTypeP(PG_GETARG_ARRAYTYPE_P(1)));
	return do_cmpfunc(query, (HTAB *) fcinfo->flinfo->fn_extra, get_fn_expr_argtype(fcinfo->flinfo, 0), fcinfo);
}

Datum zdb_anyelement_cmpfunc_array_must(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = array_to_must_query_dsl(DatumGetArrayTypeP(PG_GETARG_ARRAYTYPE_P(1)));
	return do_cmpfunc(query, (HTAB *) fcinfo->flinfo->fn_extra, get_fn_expr_argtype(fcinfo->flinfo, 0), fcinfo);
}

Datum zdb_anyelement_cmpfunc_array_not(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = array_to_not_query_dsl(DatumGetArrayTypeP(PG_GETARG_ARRAYTYPE_P(1)));
	return do_cmpfunc(query, (HTAB *) fcinfo->flinfo->fn_extra, get_fn_expr_argtype(fcinfo->flinfo, 0), fcinfo);
}

Datum zdb_anyelement_cmpfunc(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(1);
	return do_cmpfunc(query, (HTAB *) fcinfo->flinfo->fn_extra, get_fn_expr_argtype(fcinfo->flinfo, 0), fcinfo);
}

Datum zdb_tid_cmpfunc(PG_FUNCTION_ARGS) {
	ItemPointer  ctid   = (ItemPointer) PG_GETARG_POINTER(0);
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(1);
	Node         *left  = linitial(((OpExpr *) fcinfo->flinfo->fn_expr)->args);

	if (IsA(left, Var)) {
		Var           *var          = (Var *) left;
		QueryDesc     *currentQuery = linitial(currentQueryStack);
		RangeTblEntry *rentry       = rt_fetch(var->varnoold, currentQuery->plannedstmt->rtable);
		HTAB          *hash         = (HTAB *) fcinfo->flinfo->fn_extra;
		Oid           heapRelId;
		Relation      heapRel;
		bool          found;

		heapRelId = rentry->relid;

		heapRel = RelationIdGetRelation(heapRelId);
		if (hash == NULL) {
			/*
			 * execute query using our rhs argument and turn it into a hash
			 * and store that hash with this function for future evaluations
			 */
			MemoryContext oldContext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
			Relation      indexRel;

			indexRel = find_index_relation(heapRel, get_rel_type_id(heapRelId), AccessShareLock);
			hash     = create_ctid_map(heapRel, indexRel, query, fcinfo->flinfo->fn_mcxt);
			relation_close(indexRel, AccessShareLock);

			MemoryContextSwitchTo(oldContext);

			fcinfo->flinfo->fn_extra = hash;
		}
		RelationClose(heapRel);

		/* does our hash match the tuple currently being evaluated? */
		hash_search(hash, ctid, HASH_FIND, &found);

		PG_RETURN_BOOL(found);
	} else {
		elog(ERROR, "zombodb tid comparision function lhs is not a direct ctid column reference");
	}
	/*lint -e533 we either return a bool or elog(ERROR)*/
}

