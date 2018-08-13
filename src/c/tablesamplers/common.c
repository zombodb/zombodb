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

#include "common.h"

#include "json/json_support.h"

#include "optimizer/clauses.h"
#include "optimizer/cost.h"

void common_SampleScanGetSampleSize(PlannerInfo *root, RelOptInfo *baserel, List *paramexprs, BlockNumber *pages, double *tuples) {
	Node   *limitnode;
	double ntuples;
	double npages;

	/* Try to extract an estimate for the limit rowcount */
	limitnode = (Node *) lsecond(paramexprs);
	limitnode = estimate_expression_value(root, limitnode);

	if (IsA(limitnode, Const) &&
		!((Const *) limitnode)->constisnull) {
		ntuples = DatumGetInt64(((Const *) limitnode)->constvalue);
		if (ntuples < 0) {
			/* Default ntuples if the value is bogus */
			ntuples = 1000;
		}
	} else {
		/* Default ntuples if we didn't obtain a non-null Const */
		ntuples = 1000;
	}

	/* Clamp to the estimated relation size */
	if (ntuples > baserel->tuples)
		ntuples = baserel->tuples;

	ntuples = clamp_row_est(ntuples);

	if (baserel->tuples > 0 && baserel->pages > 0) {
		/* Estimate number of pages visited based on tuple density */
		double density = baserel->tuples / (double) baserel->pages;

		npages = ntuples / density;
	} else {
		/* For lack of data, assume one tuple per page */
		npages = ntuples;
	}

	/* Clamp to sane value */
	npages = clamp_row_est(Min((double) baserel->pages, npages));

	*pages  = (BlockNumber) npages;
	*tuples = ntuples;
}

ZDBTableSampleContext *makeTableSampleContext(char *response) {
	ZDBTableSampleContext *context = palloc(sizeof(ZDBTableSampleContext));
	void                  *json,
						  *aggregations,
						  *the_agg,
						  *sub_agg,
						  *buckets = NULL;
	int                   nelems   = 0;

	json = parse_json_object_from_string(response, CurrentMemoryContext);

	aggregations = get_json_object_object(json, "aggregations", true);
	if (aggregations != NULL) {
		the_agg = get_json_object_object(aggregations, "the_agg", true);
		if (the_agg != NULL) {
			sub_agg = get_json_object_object(the_agg, "sub_agg", true);

			if (sub_agg == NULL)
				sub_agg = the_agg;

			buckets = get_json_object_array(sub_agg, "buckets", true);
			if (buckets != NULL) {
				nelems = get_json_array_length(buckets);
			}
		}
	}

	context->json     = json;
	context->buckets  = buckets;
	context->nelems   = nelems;
	context->currelem = 0;
	context->block    = InvalidBlockNumber;

	return context;
}

/*lint -esym 715,eflags ignore unused param */
void common_InitSampleScan(SampleScanState *node, int eflags) {
	node->tsm_state = NULL;
}

BlockNumber common_NextSampleBlock(SampleScanState *node) {
	ZDBTableSampleContext *context = node->tsm_state;
	uint64                ctid;
	void                  *elem;

	if (context->currelem == context->nelems)
		return InvalidBlockNumber;

	elem = get_json_array_element_object(context->buckets, context->currelem, CurrentMemoryContext);
	ctid = get_json_object_uint64(elem, "key");
	context->block = (BlockNumber) (ctid >> 32);

	return context->block;
}

/*lint -esym 715,blockno,maxoffset ignore unused param */
OffsetNumber common_NextSampleTuple(SampleScanState *node, BlockNumber blockno, OffsetNumber maxoffset) {
	ZDBTableSampleContext *context = node->tsm_state;
	uint64                ctid;
	void                  *elem;

	if (context->currelem == context->nelems)
		return InvalidOffsetNumber;

	elem = get_json_array_element_object(context->buckets, context->currelem, CurrentMemoryContext);
	ctid = get_json_object_uint64(elem, "key");

	if (context->block != (BlockNumber) (ctid >> 32))
		return InvalidOffsetNumber;

	context->currelem++;
	return (OffsetNumber) ctid;
}

void common_EndSampleScan(SampleScanState *node) {
	if (node->tsm_state != NULL) {
		ZDBTableSampleContext *context = node->tsm_state;

		if (context->json)
			pfree(context->json);

		pfree(context);
	}
}
