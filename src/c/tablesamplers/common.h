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

#ifndef __ZDB_COMMON_H__
#define __ZDB_COMMON_H__

#include "zombodb.h"
#include "nodes/relation.h"

typedef struct ZDBTableSampleContext {
	int         nelems;
	int         currelem;
	void        *json;
	void        *buckets;
	BlockNumber block;
} ZDBTableSampleContext;

ZDBTableSampleContext *makeTableSampleContext(char *response);
void common_SampleScanGetSampleSize(PlannerInfo *root, RelOptInfo *baserel, List *paramexprs, BlockNumber *pages, double *tuples);
void common_InitSampleScan(SampleScanState *node, int eflags);
BlockNumber common_NextSampleBlock(SampleScanState *node);
OffsetNumber common_NextSampleTuple(SampleScanState *node, BlockNumber blockno, OffsetNumber maxoffset);
void common_EndSampleScan(SampleScanState *node);

#endif /* __ZDB_COMMON_H__ */
