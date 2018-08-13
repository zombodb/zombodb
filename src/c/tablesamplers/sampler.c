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

#include "sampler.h"

#include "elasticsearch/elasticsearch.h"
#include "tablesamplers/common.h"

/*lint -esym 715,seed ignore unused param */
void sampler_BeginSampleScan(SampleScanState *node, Datum *params, int nparams, uint32 seed) {
	Relation     indexRel;
	uint32       shard_size;
	ZDBQueryType *query;
	char         *response;

	Assert(nparams == 3);

	indexRel   = zdb_open_index(DatumGetObjectId(params[0]), AccessShareLock);
	shard_size = DatumGetUInt32(params[1]);
	query      = (ZDBQueryType *) DatumGetPointer(params[2]);

	response = ElasticsearchSampler(indexRel, shard_size, query);
	node->tsm_state = makeTableSampleContext(response);

	relation_close(indexRel, AccessShareLock);
}
