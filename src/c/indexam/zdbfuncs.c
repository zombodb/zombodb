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

#include "elasticsearch/elasticsearch.h"
#include "indexam/zdbam.h"

#include "access/xact.h"
#include "nodes/relation.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(zdb_index_name);
PG_FUNCTION_INFO_V1(zdb_index_url);
PG_FUNCTION_INFO_V1(zdb_index_type_name);
PG_FUNCTION_INFO_V1(zdb_request);
PG_FUNCTION_INFO_V1(zdb_restrict);
PG_FUNCTION_INFO_V1(zdb_query_srf);
PG_FUNCTION_INFO_V1(zdb_query_tids);
PG_FUNCTION_INFO_V1(zdb_profile_query);
PG_FUNCTION_INFO_V1(zdb_to_query_dsl);
PG_FUNCTION_INFO_V1(zdb_json_build_object_wrapper);
PG_FUNCTION_INFO_V1(zdb_internal_visibility_clause);

Datum zdb_index_name(PG_FUNCTION_ARGS) {
	Oid      indexRelId = PG_GETARG_OID(0);
	Relation indexRel;
	char     *indexName;

	indexRel  = zdb_open_index(indexRelId, AccessShareLock);
	indexName = pstrdup(ZDBIndexOptionsGetIndexName(indexRel));
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(CStringGetTextDatum(indexName));
}

Datum zdb_index_url(PG_FUNCTION_ARGS) {
	Oid      indexRelId = PG_GETARG_OID(0);
	Relation indexRel;
	char     *url;

	indexRel = zdb_open_index(indexRelId, AccessShareLock);
	url      = pstrdup(ZDBIndexOptionsGetUrl(indexRel));
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(CStringGetTextDatum(url));
}

Datum zdb_index_type_name(PG_FUNCTION_ARGS) {
	Oid      indexRelId = PG_GETARG_OID(0);
	Relation indexRel;
	char     *url;

	indexRel = zdb_open_index(indexRelId, AccessShareLock);
	url      = pstrdup(ZDBIndexOptionsGetTypeName(indexRel));
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(CStringGetTextDatum(url));
}

Datum zdb_request(PG_FUNCTION_ARGS) {
	Oid        indexRelId = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	char       *endpoint  = PG_ARGISNULL(1) ? NULL : GET_STR(PG_GETARG_TEXT_P(1));
	char       *method    = PG_ARGISNULL(2) ? NULL : GET_STR(PG_GETARG_TEXT_P(2));
	StringInfo postData   = NULL;
	Relation   indexRel;
	char       *response;

	if (indexRelId == InvalidOid || method == NULL || endpoint == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("Only the 'post_data' argument can be null")));
	}

	if (!PG_ARGISNULL(3)) {
		postData = makeStringInfo();
		appendStringInfo(postData, "%s", GET_STR(PG_GETARG_TEXT_P(3)));
	}

	indexRel = zdb_open_index(indexRelId, AccessShareLock);
	response = ElasticsearchArbitraryRequest(indexRel, method, endpoint, postData);
	if (is_json(response)) {
		Datum jb     = DirectFunctionCall1(jsonb_in, CStringGetDatum(response));
		Datum pretty = DirectFunctionCall1(jsonb_pretty, jb);

		response = TextDatumGetCString(pretty);
	}
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(CStringGetTextDatum(response));
}

Datum zdb_restrict(PG_FUNCTION_ARGS) {
	PlannerInfo      *root         = (PlannerInfo *) PG_GETARG_POINTER(0);
//	Oid              operator    = PG_GETARG_OID(1);
	List             *args         = (List *) PG_GETARG_POINTER(2);
	int              varRelid      = PG_GETARG_INT32(3);
	float8           selectivity   = 0.0;
	Node             *left         = (Node *) linitial(args);
	Node             *right        = (Node *) lsecond(args);
	Oid              heapRelId     = InvalidOid;
	VariableStatData ldata;
	Relation         heapRel;
	uint64           countEstimate = 1;

	if (IsA(left, Var)) {
		examine_variable(root, left, varRelid, &ldata);

		if (ldata.vartype == TIDOID && ldata.rel != NULL) {
			RangeTblEntry *rentry = planner_rt_fetch(ldata.rel->relid, root);

			heapRelId = rentry->relid;
		}
	}

	if (heapRelId != InvalidOid) {
		heapRel = RelationIdGetRelation(heapRelId);

		/* if 'right' is a Const we can estimate the selectivity of the query to be executed */
		if (IsA(right, Const)) {
			Const *rconst = (Const *) right;

			if (type_is_array(rconst->consttype)) {
				countEstimate = (uint64) zdb_default_row_estimation_guc;
			} else {
				ZDBQueryType *zdbquery = (ZDBQueryType *) DatumGetPointer(rconst->constvalue);
				uint64       estimate  = zdbquery_get_row_estimate(zdbquery);

				if (estimate < 1) {
					/* we need to ask Elasticsearch to estimate our selectivity */
					Relation indexRel;

					/*lint -esym 644,ldata  ldata is defined above in the if (IsA(Var)) block */
					indexRel      = find_zombodb_index(heapRel);
					countEstimate = ElasticsearchEstimateSelectivity(indexRel, zdbquery);
					relation_close(indexRel, AccessShareLock);
				} else {
					/* we'll just use the hardcoded value in the query */
					if (estimate > 1)
						countEstimate = estimate;
				}
			}
		}

		/* Assume we'll always return at least 1 row */
		selectivity = (float8) countEstimate / (float8) Max(heapRel->rd_rel->reltuples, 1.0);
		RelationClose(heapRel);
	}

	/* keep selectivity in bounds */
	selectivity = Max(0, Min(1, selectivity));

	PG_RETURN_FLOAT8(selectivity);
}

Datum zdb_query_srf(PG_FUNCTION_ARGS) {
	FuncCallContext            *funcctx;
	MemoryContext              oldcontext;
	ElasticsearchScrollContext *scrollContext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL()) {
		Oid          indexRelOid = PG_GETARG_OID(0);
		ZDBQueryType *zdbquery   = (ZDBQueryType *) PG_GETARG_VARLENA_P(1);
		Relation     indexRel;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* open target relations */
		indexRel = zdb_open_index(indexRelOid, AccessShareLock);

		/* start a query against Elasticsearch, in the proper memory context for this SRF */
		oldcontext    = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		scrollContext = ElasticsearchOpenScroll(indexRel, zdbquery, false, false, 0, NULL, NULL, 0);
		MemoryContextSwitchTo(oldcontext);

		relation_close(indexRel, AccessShareLock);

		if (scrollContext->total == 0) {
			/* fast track when no results */
			ElasticsearchCloseScroll(scrollContext);

			SRF_RETURN_DONE(funcctx);
		} else {
			/* got results, keep track of them */
			funcctx->max_calls = scrollContext->total;
			funcctx->user_fctx = scrollContext;
		}
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls) {
		/* we have more rows return */
		ItemPointer   ctid;
		MemoryContext oldContext;

		scrollContext = (ElasticsearchScrollContext *) funcctx->user_fctx;

		/*
		 * must switch MemoryContexts before we talk to ES because we might
		 * allocate more memory that we need for subsequent calls into this SRF
		 */
		oldContext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		ctid = palloc(sizeof(ItemPointerData));
		ElasticsearchGetNextItemPointer(scrollContext, ctid, NULL, NULL, NULL);

		MemoryContextSwitchTo(oldContext);
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(ctid));
	} else {
		/* all done */
		ElasticsearchCloseScroll((ElasticsearchScrollContext *) funcctx->user_fctx);
		SRF_RETURN_DONE(funcctx);
	}
}

Datum zdb_query_tids(PG_FUNCTION_ARGS) {
	Oid                        indexRelOid    = PG_GETARG_OID(0);
	ZDBQueryType               *userJsonQuery = (ZDBQueryType *) PG_GETARG_POINTER(1);
	ArrayBuildState            *astate        = NULL;
	ElasticsearchScrollContext *scrollContext;
	Relation                   indexRel;
	uint64                     i;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);

	scrollContext = ElasticsearchOpenScroll(indexRel, userJsonQuery, false, false, 0, NULL,
											NULL, 0);

	relation_close(indexRel, AccessShareLock);

	for (i = 0; i < scrollContext->total; i++) {
		ItemPointerData ctid;

		ElasticsearchGetNextItemPointer(scrollContext, &ctid, NULL, NULL, NULL);

		astate = accumArrayResult(astate,
								  PointerGetDatum(&ctid),
								  false,
								  TIDOID,
								  CurrentMemoryContext);
	}
	ElasticsearchCloseScroll(scrollContext);

	if (astate == NULL)
		astate = initArrayResult(TIDOID, CurrentMemoryContext, false);

	PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate, CurrentMemoryContext));
}

Datum zdb_profile_query(PG_FUNCTION_ARGS) {
	Oid          indexRelOid = PG_GETARG_OID(0);
	ZDBQueryType *query      = (ZDBQueryType *) PG_GETARG_VARLENA_P(1);
	Relation     indexRel;
	char         *response;

	indexRel = zdb_open_index(indexRelOid, AccessShareLock);
	response = ElasticsearchProfileQuery(indexRel, query);
	relation_close(indexRel, AccessShareLock);

	PG_RETURN_TEXT_P(CStringGetTextDatum(response));
}

Datum zdb_to_query_dsl(PG_FUNCTION_ARGS) {
	ZDBQueryType *query = (ZDBQueryType *) PG_GETARG_VARLENA_P(0);
	char         *dsl   = zdbquery_get_query(query);

	PG_RETURN_POINTER(DirectFunctionCall1(json_in, CStringGetDatum(dsl)));
}

Datum zdb_json_build_object_wrapper(PG_FUNCTION_ARGS) {
	return json_build_object(fcinfo);
}

Datum zdb_internal_visibility_clause(PG_FUNCTION_ARGS) {
	MemoryContext tmpContext = AllocSetContextCreate(CurrentMemoryContext, "visibility_clause", ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(tmpContext);
	ZDBQueryType  *output;

	{
		Oid             indexRelOid = PG_GETARG_OID(0);
		Snapshot        snapshot    = GetTransactionSnapshot();
		CommandId       commandId   = GetCurrentCommandId(false);
		ArrayType       *myXids     = collect_used_xids(tmpContext);
		uint64          xmax        = convert_xid(snapshot->xmax);
		ArrayBuildState *astate     = initArrayResult(INT8OID, tmpContext, false);
		Relation        indexRel;
		Oid             zdb_visibility_clause;
		ZDBQueryType    *vis;
		size_t          len;

		indexRel = zdb_open_index(indexRelOid, AccessShareLock);

		/* build up an array of active transaction ids */
		if (snapshot->xcnt > 0) {
			uint32 i;

			for (i = 0; i < snapshot->xcnt; i++) {
				astate = accumArrayResult(astate, UInt64GetDatum(convert_xid(snapshot->xip[i])), false, INT8OID,
										  tmpContext);
			}
		}

		/* find our visibility query SQL function */
		zdb_visibility_clause = DatumGetObjectId(DirectFunctionCall1(regprocedurein, CStringGetDatum(
				"zdb.visibility_clause(bigint[], bigint, int, bigint[], regclass, text)")));

		/* ... and call it */
		vis = (ZDBQueryType *) DatumGetTextP(
				OidFunctionCall6(zdb_visibility_clause, PointerGetDatum(myXids), UInt64GetDatum(xmax),
								 CommandIdGetDatum(commandId),
								 makeArrayResult(astate, tmpContext),
								 ObjectIdGetDatum(RelationGetRelid(indexRel)),
								 CStringGetTextDatum(ZDBIndexOptionsGetTypeName(indexRel))));

		relation_close(indexRel, AccessShareLock);

		/* switch back to the original memory context */
		MemoryContextSwitchTo(oldContext);

		/* and copy the 'vis'ibility query into this context so we can return it */
		len    = strlen(vis->json);
		output = palloc0(sizeof(ZDBQueryType) + len + 1);
		output->vl_len_ = vis->vl_len_;
		memcpy(output->json, vis->json, len);
	}
	MemoryContextDelete(tmpContext);

	PG_RETURN_POINTER(output);
}
