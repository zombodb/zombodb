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

#include "zombodb.h"
#include "tablesamplers/common.h"
#include "tablesamplers/sampler.h"
#include "tablesamplers/diversified.h"
#include "tablesamplers/query.h"

#include "access/tsmapi.h"

PG_FUNCTION_INFO_V1(zdb_table_sampler);
PG_FUNCTION_INFO_V1(zdb_diversified_table_sampler);
PG_FUNCTION_INFO_V1(zdb_query_table_sampler);

Datum zdb_table_sampler(PG_FUNCTION_ARGS) {
	TsmRoutine *handler = makeNode(TsmRoutine);
	Oid        typeoid  = DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum("zdbquery")));

	handler->BeginSampleScan           = sampler_BeginSampleScan;
	handler->EndSampleScan             = common_EndSampleScan;
	handler->InitSampleScan            = common_InitSampleScan;
	handler->NextSampleBlock           = common_NextSampleBlock;
	handler->NextSampleTuple           = common_NextSampleTuple;
	handler->SampleScanGetSampleSize   = common_SampleScanGetSampleSize;
	handler->repeatable_across_queries = true;
	handler->repeatable_across_scans   = true;
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, REGCLASSOID);
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, INT4OID);
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, typeoid);

	PG_RETURN_POINTER(handler);
}

Datum zdb_diversified_table_sampler(PG_FUNCTION_ARGS) {
	TsmRoutine *handler = makeNode(TsmRoutine);
	Oid        typeoid  = DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum("zdbquery")));

	handler->BeginSampleScan           = diversified_BeginSampleScan;
	handler->EndSampleScan             = common_EndSampleScan;
	handler->InitSampleScan            = common_InitSampleScan;
	handler->NextSampleBlock           = common_NextSampleBlock;
	handler->NextSampleTuple           = common_NextSampleTuple;
	handler->SampleScanGetSampleSize   = common_SampleScanGetSampleSize;
	handler->repeatable_across_queries = true;
	handler->repeatable_across_scans   = true;
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, REGCLASSOID);
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, INT4OID);
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, TEXTOID);
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, typeoid);

	PG_RETURN_POINTER(handler);
}

Datum zdb_query_table_sampler(PG_FUNCTION_ARGS) {
	TsmRoutine *handler = makeNode(TsmRoutine);
	Oid        typeoid  = DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum("zdbquery")));

	handler->BeginSampleScan           = query_BeginSampleScan;
	handler->EndSampleScan             = common_EndSampleScan;
	handler->InitSampleScan            = common_InitSampleScan;
	handler->NextSampleBlock           = common_NextSampleBlock;
	handler->NextSampleTuple           = common_NextSampleTuple;
	handler->SampleScanGetSampleSize   = query_SampleScanGetSampleSize;
	handler->repeatable_across_queries = true;
	handler->repeatable_across_scans   = true;
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, REGCLASSOID);
	handler->parameterTypes            = lappend_oid(handler->parameterTypes, typeoid);

	PG_RETURN_POINTER(handler);
}
