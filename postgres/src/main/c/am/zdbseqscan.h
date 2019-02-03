/*
 * Copyright 2015-2019 ZomboDB, LLC
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
#ifndef __ZDBSEQSCAN_H__
#define __ZDBSEQSCAN_H__

#include "postgres.h"
#include "fmgr.h"
#include "zdbscore.h"

#ifndef PG_USE_INLINE
extern void set_item_pointer(ZDBSearchResponse *data, uint64 index, ItemPointer target, ZDBScore *score, bool hasScores);
#endif   /* !PG_USE_INLINE */
#if defined(PG_USE_INLINE) || defined(ZDBSEQSCAN_INCLUDE_DEFINITIONS)

STATIC_IF_INLINE void set_item_pointer(ZDBSearchResponse *data, uint64 index, ItemPointer target, ZDBScore *score, bool hasScores) {
    BlockNumber  blkno;
    OffsetNumber offno;

    if (hasScores) {
        memcpy(&blkno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber) + sizeof(float4))), sizeof(BlockNumber));
        memcpy(&offno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber) + sizeof(float4)) + sizeof(BlockNumber)), sizeof(OffsetNumber));
        memcpy(score, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber) + sizeof(float4)) + sizeof(BlockNumber) + sizeof(OffsetNumber)), sizeof(float4));
    } else {
        memcpy(&blkno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber))), sizeof(BlockNumber));
        memcpy(&offno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber)) + sizeof(BlockNumber)), sizeof(OffsetNumber));
    }
    ItemPointerSet(target, blkno, offno);
}

#endif /* PG_USE_INLINE || ZDBSEQSCAN_INCLUDE_DEFINITIONS */

extern List *SEQUENTIAL_SCAN_INDEXES;
extern HTAB *SEQUENTIAL_SCANS;
extern List *CURRENT_QUERY_STACK;

extern Datum zdb_seqscan(PG_FUNCTION_ARGS);
extern void  zdb_sequential_scan_support_cleanup(void);
extern Datum zdbsel(PG_FUNCTION_ARGS);
extern bool current_query_wants_scores(void);
extern uint64 get_row_estimate(ZDBIndexDescriptor *desc, Relation heapRel, char *query);

#endif
