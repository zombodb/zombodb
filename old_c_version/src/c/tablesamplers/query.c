/*
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

#include "query.h"

#include "elasticsearch/elasticsearch.h"
#include "tablesamplers/common.h"

#include "optimizer/cost.h"

/*lint -esym 715,root,paramexprs ignore unused param */
void query_SampleScanGetSampleSize(PlannerInfo *root, RelOptInfo *baserel, List *paramexprs, BlockNumber *pages, double *tuples) {
	double ntuples = 2500;    /* just always going to assume it's 2500 rows */
	double npages;

	/* Clamp to the estimated relation size */
	if (ntuples > baserel->tuples)
		ntuples = (int64) baserel->tuples;

	ntuples = (uint64) clamp_row_est(ntuples);

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

/*lint -esym 715,seed ignore unused param */
void query_BeginSampleScan(SampleScanState *node, Datum *params, int nparams, uint32 seed) {
	Relation     indexRel;
	ZDBQueryType *query;
	char         *response;

	Assert(nparams == 2);

	indexRel = zdb_open_index(DatumGetObjectId(params[0]), AccessShareLock);
	query    = (ZDBQueryType *) DatumGetPointer(params[1]);

	response = ElasticsearchQuerySampler(indexRel, query);
	node->tsm_state = makeTableSampleContext(response);

	relation_close(indexRel, AccessShareLock);
}
