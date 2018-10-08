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

#ifndef __ZDB_SCORING_H__
#define __ZDB_SCORING_H__

#include "zombodb.h"

typedef struct ZDBScoreKey {
	ItemPointerData ctid;
} ZDBScoreKey;

typedef struct ZDBScoreEntry {
	ZDBScoreKey key;
	float4      score;
} ZDBScoreEntry;


typedef float4 (*score_lookup_callback)(ItemPointer ctid, void *arg);

void scoring_support_init(void);
void scoring_support_cleanup(void);
HTAB *scoring_create_lookup_table(MemoryContext memoryContext, char *name);
void scoring_register_callback(Oid heapOid, score_lookup_callback callback, void *callback_data, MemoryContext memoryContext);
bool current_scan_wants_scores(IndexScanDesc scan, Relation heapRel);

#endif /* __ZDB_SCORING_H__ */
