/*
 * Copyright 2015-2017 ZomboDB, LLC
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

#define ZDBSEQSCAN_INCLUDE_DEFINITIONS

#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "zdb_interface.h"
#include "zdbseqscan.h"
#include "zdbops.h"

PG_FUNCTION_INFO_V1(zdbsel);

static uint32 sequential_scan_key_hash(const void *key, Size keysize);
static int    sequential_scan_key_match(const void *key1, const void *key2, Size keysize);
static void   *sequential_scan_key_copy(void *d, const void *s, Size keysize);

static void initialize_sequential_scan_cache(void);
static Oid  determine_index_oid(Node *node);

typedef struct SequentialScanIndexRef {
    Oid funcOid;
    Oid heapRelOid;
    Oid indexRelOid;
}           SequentialScanIndexRef;

typedef struct SequentialScanKey {
    Oid    indexRelOid;
    char   *query;
    size_t query_len;
}           SequentialScanKey;

typedef struct SequentialScanEntry {
    SequentialScanKey key;
    HTAB              *scan;
    ItemPointer       one_hit;
    bool              empty;
    ZDBScore          score;
    bool              hasScore;
}           SequentialScanEntry;

typedef struct SequentialScanTidAndScore {
    ItemPointerData tid;
    ZDBScore        score;
    bool            hasScore;
}           SequentialScanTidAndScoreEntry;

List *SEQUENTIAL_SCAN_INDEXES = NULL;
HTAB *SEQUENTIAL_SCANS        = NULL;
List *CURRENT_QUERY_STACK     = NULL;

static uint32 sequential_scan_key_hash(const void *key, Size keysize) {
    const SequentialScanKey *ssk = (const SequentialScanKey *) key;

    return string_hash(ssk->query, strlen(ssk->query) + 1) ^ ssk->indexRelOid;
}

static int sequential_scan_key_match(const void *key1, const void *key2, Size keysize) {
    const SequentialScanKey *a = (const SequentialScanKey *) key1;
    const SequentialScanKey *b = (const SequentialScanKey *) key2;
    bool                    match;

    match = a->indexRelOid == b->indexRelOid && a->query_len == b->query_len && strcmp(a->query, b->query) == 0;
    return match ? 0 : 1;
}

static void *sequential_scan_key_copy(void *d, const void *s, Size keysize) {
    const SequentialScanKey *src  = (const SequentialScanKey *) s;
    SequentialScanKey       *dest = (SequentialScanKey *) d;

    dest->indexRelOid = src->indexRelOid;
    dest->query_len   = src->query_len;
    dest->query       = MemoryContextAlloc(TopTransactionContext, src->query_len + 1);
    memcpy(dest->query, src->query, src->query_len + 1);

    return dest;
}

static void initialize_sequential_scan_cache(void) {
    /* setup our query-wide cache of sequential scans */
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(SequentialScanKey);
    ctl.entrysize = sizeof(SequentialScanEntry);
    ctl.hash      = sequential_scan_key_hash;
    ctl.match     = sequential_scan_key_match;
    ctl.keycopy   = sequential_scan_key_copy;

    SEQUENTIAL_SCANS = hash_create("zdb global sequential scan cache", 32, &ctl,
                                   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_KEYCOPY);
}

static Oid determine_index_oid(Node *node) {
    MemoryContext          oldContext;
    SequentialScanIndexRef *indexRef;
    FuncExpr               *funcExpr  = NULL;
    Oid                    funcOid    = InvalidOid;
    Oid                    heapRelOid = InvalidOid;
    Oid                    zdbIndexRel;
    ListCell               *lc;

    if (IsA(node, Var)) {
        Var *var = (Var *) node;
        switch (var->varno) {
            case INNER_VAR:
            case OUTER_VAR:
                elog(ERROR, "Cannot determine index.  Left side of operator is a Var type we don't understand");
                break;

            default: {
                QueryDesc     *query  = linitial(CURRENT_QUERY_STACK);
                RangeTblEntry *rentry = rt_fetch(var->varno, query->plannedstmt->rtable);
                heapRelOid = rentry->relid;
            }
        }
    } else {
        if (!IsA(node, FuncExpr))
            elog(ERROR, "Cannot determine index. Left side of operator is not compatible with ZomboDB.");

        funcExpr = (FuncExpr *) node;
        funcOid  = funcExpr->funcid;
        validate_zdb_funcExpr(funcExpr, &heapRelOid);
    }

    /* look in cache for the index oid */
    foreach(lc, SEQUENTIAL_SCAN_INDEXES) {
        indexRef = (SequentialScanIndexRef *) lfirst(lc);

        if (indexRef->heapRelOid == heapRelOid && indexRef->funcOid == funcOid) {
            return indexRef->indexRelOid;
        }
    }

    /* figure out what the index oid should be */
    if (funcExpr == NULL)
        zdbIndexRel = zdb_determine_index_oid_by_heap(heapRelOid);
    else
        zdbIndexRel = zdb_determine_index_oid(funcExpr, heapRelOid);

    /* and cache it */
    oldContext = MemoryContextSwitchTo(TopTransactionContext);
    indexRef   = palloc(sizeof(SequentialScanIndexRef));
    indexRef->funcOid     = funcOid;
    indexRef->heapRelOid  = heapRelOid;
    indexRef->indexRelOid = zdbIndexRel;
    SEQUENTIAL_SCAN_INDEXES = lappend(SEQUENTIAL_SCAN_INDEXES, indexRef);
    MemoryContextSwitchTo(oldContext);

    return zdbIndexRel;
}

Datum zdb_seqscan(PG_FUNCTION_ARGS) {
    ItemPointer         tid       = (ItemPointer) PG_GETARG_POINTER(0);
    char                *query    = TextDatumGetCString(PG_GETARG_TEXT_P(1));
    OpExpr              *opexpr   = (OpExpr *) fcinfo->flinfo->fn_expr;
    Node                *funcExpr = (Node *) linitial(opexpr->args);
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
        bool               wantScores = current_query_wants_scores();

        desc     = zdb_alloc_index_descriptor_by_index_oid(key.indexRelOid);
        response = desc->implementation->searchIndex(desc, &query, 1, &nhits, wantScores, false);

        entry = hash_search(SEQUENTIAL_SCANS, &key, HASH_ENTER, &found);
        entry->one_hit = NULL;
        entry->empty   = false;
        entry->scan    = NULL;

        if (nhits == 0) {
            entry->empty = true;
        } else if (nhits == 1) {
            /* optimization for a single hit -- avoids overhead of a 1-entry HTAB */
            MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

            entry->one_hit = palloc(sizeof(ItemPointerData));
            entry->hasScore = wantScores;
            set_item_pointer(response, 0, entry->one_hit, &entry->score, wantScores);

            MemoryContextSwitchTo(oldContext);
        } else {
            /* turn the response data from ES into an HTAB for efficient lookup on the next scan */
            HASHCTL ctl;

            memset(&ctl, 0, sizeof(ctl));
            ctl.keysize   = sizeof(ItemPointerData);
            ctl.entrysize = sizeof(SequentialScanTidAndScoreEntry);
            ctl.hash      = tag_hash;
            ctl.hcxt      = TopTransactionContext;

            entry->scan = hash_create(query, nhits, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

            for (i = 0; i < nhits; i++) {
                ItemPointerData                hit_tid;
                ZDBScore                       hit_score;
                SequentialScanTidAndScoreEntry *tid_and_score_entry;

                CHECK_FOR_INTERRUPTS();

                set_item_pointer(response, i, &hit_tid, &hit_score, wantScores);
                tid_and_score_entry = hash_search(entry->scan, &hit_tid, HASH_ENTER, &found);
                if (wantScores) {
                    tid_and_score_entry->score    = hit_score;
                    tid_and_score_entry->hasScore = wantScores;
                }
            }
        }

        desc->implementation->freeSearchResponse(response);
    }

    /* does the tid being sequentially scanned match the query? */
    if (entry->empty) {
        found = false;
    } else if (entry->one_hit) {
        found = ItemPointerEquals(tid, entry->one_hit);
        if (found && entry->hasScore)
            zdb_record_score(key.indexRelOid, tid, entry->score);
    } else {
        SequentialScanTidAndScoreEntry *tid_and_score;

        tid_and_score = hash_search(entry->scan, tid, HASH_FIND, &found);
        if (found && tid_and_score->hasScore)
            zdb_record_score(key.indexRelOid, tid, tid_and_score->score);
    }

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
        FuncExpr *funcExpr     = (FuncExpr *) funcArg;
        Const    *queryConst   = (Const *) queryNode;
        Node     *regclassNode = (Node *) linitial(funcExpr->args);

        if (IsA(regclassNode, Const)) {
            Const              *tableRegclass = (Const *) regclassNode;
            Oid                heapRelOid     = (Oid) DatumGetObjectId(tableRegclass->constvalue);
            char               *query         = TextDatumGetCString(queryConst->constvalue);
            Relation           heapRel;
            ZDBIndexDescriptor *desc;
            uint64             nhits;

            heapRel     = RelationIdGetRelation(heapRelOid);
            desc        = zdb_alloc_index_descriptor_by_index_oid(determine_index_oid((Node *) funcExpr));

			nhits       = get_row_estimate(desc, heapRel, query);
			selectivity = ((float8) nhits) / heapRel->rd_rel->reltuples;

            RelationClose(heapRel);
        }
    }

    selectivity = Min(selectivity, 1);
    selectivity = Max(selectivity, 0);

    PG_RETURN_FLOAT8(selectivity);
}


bool current_query_wants_scores(void) {
    QueryDesc *queryDesc  = (QueryDesc *) linitial(CURRENT_QUERY_STACK);
    Oid        args[2]       = {REGCLASSOID, TIDOID};
    Oid        zdb_score_oid = LookupFuncName(lappend(NULL, makeString("zdb_score_internal")), 2, args, true);
    StringInfo find          = makeStringInfo();
    char       *str;

    // TODO:  how to walk the plan tree without converting it to a string
    // TODO:  and otherwise implementing a ton of code that'll be impossible
    // TODO:  to keep current between Postgres versions
    str = nodeToString(queryDesc->plannedstmt->planTree);

    /*
     * We're looking for a function expression (FuncExpr) somewhere
     * in the plan that references our zdb_score_internal function
     */
    appendStringInfo(find, "{FUNCEXPR :funcid %d ", zdb_score_oid);
    return strstr(str, find->data) != 0;
}

uint64 get_row_estimate(ZDBIndexDescriptor *desc, Relation heapRel, char *query) {
	uint64 estimate;

	if (zdb_force_row_estimates_guc || desc->defaultRowEstimate == -1) {
		/* go to the remote index and estimate */
		estimate = desc->implementation->estimateSelectivity(desc, query);
	} else {
		/* use the default value specified on the local PG index */
		estimate     = Max(1, (uint64) desc->defaultRowEstimate);

		/* unless it's greater than the number of tuples we think we have */
		if (estimate > heapRel->rd_rel->reltuples)
			estimate = 1;
	}

	/* make sure it's always at least 1 */
	return Max(1, estimate);
}
