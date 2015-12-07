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
#ifndef __ZDBSCORE_H__
#define __ZDBSCORE_H__

#include "postgres.h"
#include "fmgr.h"

typedef struct {
    union {
        int32 iscore;
        float4 fscore;
    };
} ZDBScore;

#define ZDB_ALLOC_SCORE() \
do {\
   if (zdb_last_score == NULL) zdb_last_score = MemoryContextAlloc(TopTransactionContext, sizeof(ZDBScore)); \
} while(0)

#define ZDB_SET_SCORE(s) \
do {\
  ZDB_ALLOC_SCORE(); \
  memcpy(zdb_last_score, &s, sizeof(ZDBScore)); \
} while(0)

#define ZDB_RESET_SCORE() zdb_last_score = NULL;

extern Datum zdb_score(PG_FUNCTION_ARGS);

extern ZDBScore *zdb_last_score;

#endif