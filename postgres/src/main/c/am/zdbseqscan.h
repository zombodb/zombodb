/*
 * Copyright 2015 ZomboDB, LLC
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

__inline static void set_item_pointer(ZDBSearchResponse *data, uint64 index, ItemPointer target)
{
    BlockNumber  blkno;
    OffsetNumber offno;

    memcpy(&blkno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber))), sizeof(BlockNumber));
    memcpy(&offno, data->hits + (index * (sizeof(BlockNumber) + sizeof(OffsetNumber)) + sizeof(BlockNumber)), sizeof(OffsetNumber));

    ItemPointerSet(target, blkno, offno);
}

extern List *SEQUENTIAL_SCAN_INDEXES;
extern HTAB *SEQUENTIAL_SCANS;

extern Datum zdb_seqscan(PG_FUNCTION_ARGS);
extern void zdb_sequential_scan_support_cleanup(void);
extern Datum zdbcostestimate(PG_FUNCTION_ARGS);
extern Datum zdbsel(PG_FUNCTION_ARGS);

#endif
