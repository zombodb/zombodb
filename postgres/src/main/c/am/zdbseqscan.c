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

#include "postgres.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "util/zdbutils.h"
#include "zdb_interface.h"
#include "zdbseqscan.h"

PG_FUNCTION_INFO_V1(zdbsel);

static uint32 sequential_scan_key_hash(const void *key, Size keysize);
static int    sequential_scan_key_match(const void *key1, const void *key2, Size keysize);
static void   *sequential_scan_key_copy(void *d, const void *s, Size keysize);

static void initialize_sequential_scan_cache(void);
static Oid  determine_index_oid(Node *funcExpr);

typedef struct SequentialScanIndexRef {
    Oid funcOid;
    Oid indexRelOid;
}           SequentialScanIndexRef;

typedef struct SequentialScanKey {
    Oid    indexRelOid;
    char   *query;
    size_t query_len;
}           SequentialScanKey;

typedef struct SequentialScanEntry {
    HTAB        *scan;
    ItemPointer one_hit;
    bool        empty;
}           SequentialScanEntry;

List *SEQUENTIAL_SCAN_INDEXES = NULL;
HTAB *SEQUENTIAL_SCANS        = NULL;

static uint32 sequential_scan_key_hash(const void *key, Size keysize) {
    SequentialScanKey *ssk = (SequentialScanKey *) key;

    return string_hash(ssk->query, strlen(ssk->query) + 1);
}

static int sequential_scan_key_match(const void *key1, const void *key2, Size keysize) {
    SequentialScanKey *a = (SequentialScanKey *) key1;
    SequentialScanKey *b = (SequentialScanKey *) key2;

    return a->indexRelOid == b->indexRelOid && strcmp(a->query, b->query) == 0;
}

static void *sequential_scan_key_copy(void *d, const void *s, Size keysize) {
    SequentialScanKey *src  = (SequentialScanKey *) s;
    SequentialScanKey *dest = (SequentialScanKey *) d;

    dest->indexRelOid = src->indexRelOid;
    dest->query       = palloc(src->query_len + 1);
    memcpy(dest->query, src->query, src->query_len + 1);

    return dest;
}

static void initialize_sequential_scan_cache(void) {
    /* setup our query-wide cache of sequential scans */
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(HASHCTL));
    ctl.keysize   = sizeof(SequentialScanKey);
    ctl.entrysize = sizeof(SequentialScanEntry);
    ctl.hash      = sequential_scan_key_hash;
    ctl.match     = sequential_scan_key_match;
    ctl.keycopy   = sequential_scan_key_copy;
    ctl.hcxt      = TopTransactionContext;

    SEQUENTIAL_SCANS = hash_create("zdb global sequential scan cache", 32, &ctl,
                                   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_KEYCOPY);
}

static Oid determine_index_oid(Node *node) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    FuncExpr      *funcExpr;
    Const         *tableRegclass;
    Oid           heapRelOid;
    Relation      heapRel;
    List          *indexes;
    ListCell      *lc;

    if (!IsA(node, FuncExpr))
        elog(ERROR, "Cannot determine index. Left side of operator is not compatible with ZomboDB.");

    funcExpr      = (FuncExpr *) node;
    tableRegclass = (Const *) linitial(funcExpr->args);
    heapRelOid    = (Oid) DatumGetObjectId(tableRegclass->constvalue);

    foreach(lc, SEQUENTIAL_SCAN_INDEXES) {
        SequentialScanIndexRef *indexRef = (SequentialScanIndexRef *) lfirst(lc);

        if (indexRef->funcOid == funcExpr->funcid)
            return indexRef->indexRelOid;
    }

    heapRel = RelationIdGetRelation(heapRelOid);
    indexes = RelationGetIndexList(heapRel);
    foreach(lc, indexes) {
        Relation indexRel;
        Oid      indexRelOid;

        indexRelOid = (Oid) lfirst(lc);
        indexRel    = RelationIdGetRelation(indexRelOid);
        if (strcmp("zombodb", indexRel->rd_am->amname.data) == 0) {
            List     *indexExpressions = RelationGetIndexExpressions(indexRel);
            ListCell *lc2;

            foreach (lc2, indexExpressions) {
                Node *n = (Node *) lfirst(lc2);

                if (IsA(n, FuncExpr)) {
                    FuncExpr *indexFuncExpr = (FuncExpr *) n;
                    if (indexFuncExpr->funcid == funcExpr->funcid) {
                        SequentialScanIndexRef *indexRef;

                        indexRef = palloc(sizeof(SequentialScanIndexRef));
                        indexRef->funcOid     = funcExpr->funcid;
                        indexRef->indexRelOid = indexRelOid;
                        SEQUENTIAL_SCAN_INDEXES = lappend(SEQUENTIAL_SCAN_INDEXES, indexRef);

                        RelationClose(indexRel);
                        RelationClose(heapRel);

                        MemoryContextSwitchTo(oldContext);
                        return indexRelOid;
                    }
                }
            }
        }
        RelationClose(indexRel);
    }
    RelationClose(heapRel);

    MemoryContextSwitchTo(oldContext);

    elog(ERROR, "Unable to find ZomboDB index for '%s'", RelationGetRelationName(heapRel));
    return InvalidOid;
}

Datum zdb_seqscan(PG_FUNCTION_ARGS) {
    MemoryContext       oldContext = MemoryContextSwitchTo(TopTransactionContext);
    ItemPointer         tid        = (ItemPointer) PG_GETARG_POINTER(0);
    char                *query     = TextDatumGetCString(PG_GETARG_TEXT_P(1));
    OpExpr              *opexpr    = (OpExpr *) fcinfo->flinfo->fn_expr;
    Node                *funcExpr  = (Node *) linitial(opexpr->args);
    SequentialScanEntry *entry;
    SequentialScanKey   key;
    bool                found;

    if (SEQUENTIAL_SCANS == NULL)
        initialize_sequential_scan_cache();

    /* search for an active sequential scan for this index and query */
    key.indexRelOid = determine_index_oid(funcExpr);
    key.query       = query;
    key.query_len   = strlen(query);

    entry = hash_search(SEQUENTIAL_SCANS, &key, HASH_FIND, &found);
    if (!found) {
        /* no scan found for this index/query, so go query Elasticsearch */
        ZDBSearchResponse  *response;
        ZDBIndexDescriptor *desc;
        uint64             nhits, i;
        SequentialScanKey  *newKey;

        desc     = zdb_alloc_index_descriptor_by_index_oid(key.indexRelOid);
        response = desc->implementation->searchIndex(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), &query, 1, &nhits);

        newKey = palloc(sizeof(SequentialScanKey));
        memcpy(newKey, &key, sizeof(SequentialScanKey));
        entry = hash_search(SEQUENTIAL_SCANS, newKey, HASH_ENTER, &found);
        entry->one_hit = NULL;
        entry->empty   = false;
        entry->scan    = NULL;

        if (nhits == 0) {
            entry->empty = true;
        } else if (nhits == 1) {
            /* optimization for a single hit -- avoids overhead of a 1-entry HTAB */
            entry->one_hit = palloc(sizeof(ItemPointerData));
            set_item_pointer(response, 0, entry->one_hit);
        } else {
            /* turn the response data from ES into an HTAB for efficient lookup on the next scan */
            HASHCTL ctl;

            memset(&ctl, 0, sizeof(HASHCTL));
            ctl.keysize   = sizeof(ItemPointerData);
            ctl.entrysize = ctl.keysize;
            ctl.hash      = tag_hash;
            ctl.hcxt      = TopTransactionContext;

            entry->scan = hash_create(query, nhits, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

            for (i = 0; i < nhits; i++) {
                ItemPointerData data;

                CHECK_FOR_INTERRUPTS();
                set_item_pointer(response, i, &data);
                hash_search(entry->scan, &data, HASH_ENTER, &found);
            }
        }

        desc->implementation->freeSearchResponse(response);
    }

    /* does the tid being sequentially scanned match the query? */
    if (entry->empty)
        found = false;
    else if (entry->one_hit)
        found = ItemPointerEquals(tid, entry->one_hit);
    else
        hash_search(entry->scan, tid, HASH_FIND, &found);

    MemoryContextSwitchTo(oldContext);
    PG_RETURN_BOOL(found);
}

void zdb_sequential_scan_support_cleanup(void) {
    if (SEQUENTIAL_SCANS != NULL) {
        hash_destroy(SEQUENTIAL_SCANS);
        SEQUENTIAL_SCANS = NULL;
    }

    if (SEQUENTIAL_SCAN_INDEXES != NULL) {
        pfree(SEQUENTIAL_SCAN_INDEXES);
        SEQUENTIAL_SCAN_INDEXES = NULL;
    }
}

Datum zdbsel(PG_FUNCTION_ARGS) {
    List   *args       = (List *) PG_GETARG_POINTER(2);
    Node   *funcArg    = (Node *) linitial(args);
    Node   *queryNode  = (Node *) lsecond(args);
    float8 selectivity = 0.0001f;

    if (IsA(funcArg, FuncExpr) && IsA(queryNode, Const)) {
        FuncExpr           *funcExpr     = (FuncExpr *) funcArg;
        Const              *queryConst   = (Const *) queryNode;
        Node               *regclassNode = (Node *) linitial(funcExpr->args);

        if (IsA(regclassNode, Const)) {
            Const              *tableRegclass = (Const *) regclassNode;
            Oid                heapRelOid     = (Oid) DatumGetObjectId(tableRegclass->constvalue);
            char               *query         = TextDatumGetCString(queryConst->constvalue);
            Relation           heapRel;
            ZDBIndexDescriptor *desc;
            uint64             nhits;

            heapRel     = RelationIdGetRelation(heapRelOid);
            desc        = zdb_alloc_index_descriptor_by_index_oid(determine_index_oid((Node *) funcExpr));
            nhits       = desc->implementation->estimateSelectivity(desc, query);
            selectivity = ((float8) nhits) / heapRel->rd_rel->reltuples;

            RelationClose(heapRel);
        }
    }

    selectivity = Min(selectivity, 1);
    selectivity = Max(selectivity, 0);

    PG_RETURN_FLOAT8(selectivity);
}