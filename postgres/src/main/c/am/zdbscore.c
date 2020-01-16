/*
 * Copyright 2015-2020 ZomboDB, LLC
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

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "storage/itemptr.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "zdbscore.h"

PG_FUNCTION_INFO_V1(zdb_score_internal);

typedef struct ZDBBitmapScoreKey {
    ItemPointerData ctid;
    Oid             index_relid;

} ZDBBitmapScoreKey;

typedef struct ZDBBitmapScoreEntry {
    ZDBBitmapScoreKey key;
    ZDBScore          score;
} ZDBBitmapScoreEntry;

static ZDBScore *zdb_lookup_score(Oid index_relid, ItemPointer ctid);

static HTAB *bitmapScores = NULL;

static uint32 zdbbitmapscorekey_hash(const void *key, Size keysize) {
    ZDBBitmapScoreKey *k = (ZDBBitmapScoreKey *) key;

    return (uint32) (k->index_relid ^ 31 +
                                      ItemPointerGetBlockNumber(&k->ctid) ^ 31 +
                                                                            ItemPointerGetOffsetNumber(&k->ctid));
}

static int zdbbitmapscorekey_match(const void *key1, const void *key2, Size keysize) {
    ZDBBitmapScoreKey *a = (ZDBBitmapScoreKey *) key1;
    ZDBBitmapScoreKey *b = (ZDBBitmapScoreKey *) key2;

    return (a->index_relid == b->index_relid && ItemPointerEquals(&a->ctid, &b->ctid)) ? 0 : 1;
}

static void *zdbbitmapscorekey_copy(void *d, const void *s, Size keysize) {
    ZDBBitmapScoreKey *src  = (ZDBBitmapScoreKey *) s;
    ZDBBitmapScoreKey *dest = (ZDBBitmapScoreKey *) d;

    dest->index_relid = src->index_relid;
    ItemPointerCopy(&src->ctid, &dest->ctid);

    return dest;
}

void zdb_reset_scores(void) {
    bitmapScores = NULL;
}

void zdb_record_score(Oid index_relid, ItemPointer ctid, ZDBScore score) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    bool                found;
    ZDBBitmapScoreKey   key;
    ZDBBitmapScoreEntry *entry;

    if (bitmapScores == NULL) {
        HASHCTL ctl;

        memset(&ctl, 0, sizeof(ctl));
        ctl.keysize   = sizeof(ZDBBitmapScoreKey);
        ctl.entrysize = sizeof(ZDBBitmapScoreEntry);
        ctl.hcxt      = TopTransactionContext;
        ctl.hash      = zdbbitmapscorekey_hash;
        ctl.match     = zdbbitmapscorekey_match;
        ctl.keycopy   = zdbbitmapscorekey_copy;

        bitmapScores = hash_create("zdb bitmap scores", 100, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_KEYCOPY);
    }

    key.index_relid = index_relid;
    memcpy(&key.ctid, ctid, sizeof(ItemPointerData));
    entry = hash_search(bitmapScores, &key, HASH_ENTER, &found);
    memcpy(&entry->score, &score, sizeof(ZDBScore));

    MemoryContextSwitchTo(oldContext);
}

static ZDBScore *zdb_lookup_score(Oid index_relid, ItemPointer ctid) {

    if (bitmapScores != NULL) {
        ZDBBitmapScoreKey   key;
        ZDBBitmapScoreEntry *entry;
        bool                found;

        key.index_relid = index_relid;
        memcpy(&key.ctid, ctid, sizeof(ItemPointerData));
        entry = hash_search(bitmapScores, &key, HASH_FIND, &found);
        if (found) {
            return &entry->score;
        }
    }

    return NULL;
}

Datum zdb_score_internal(PG_FUNCTION_ARGS) {
    Oid         index_relid = PG_GETARG_OID(0);
    ItemPointer ctid        = (ItemPointer) PG_GETARG_POINTER(1);
    ZDBScore    *score;

    score = zdb_lookup_score(index_relid, ctid);
    if (score == NULL)
        elog(ERROR, "scores were not detected as necessary for this query");

    PG_RETURN_FLOAT4(score->fscore);
}
