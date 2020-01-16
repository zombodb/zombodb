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

#include "diversified.h"

#include "elasticsearch/elasticsearch.h"
#include "tablesamplers/common.h"

/*lint -esym 715,seed ignore unused param */
void diversified_BeginSampleScan(SampleScanState *node, Datum *params, int nparams, uint32 seed) {
	Relation     indexRel;
	uint32       shard_size;
	char         *field;
	ZDBQueryType *query;
	char         *response;

	Assert(nparams == 4);

	indexRel   = zdb_open_index(DatumGetObjectId(params[0]), AccessShareLock);
	shard_size = DatumGetUInt32(params[1]);
	field      = TextDatumGetCString(params[2]);
	query      = (ZDBQueryType *) DatumGetPointer(params[3]);

	response = ElasticsearchDiversifiedSampler(indexRel, shard_size, field, query);
	node->tsm_state = makeTableSampleContext(response);

	relation_close(indexRel, AccessShareLock);
}
