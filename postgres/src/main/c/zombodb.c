/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
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
#include <sys/time.h>

#include "postgres.h"
#include "fmgr.h"
#include "pgstat.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/relscan.h"
#include "access/tuptoaster.h"
#include "access/visibilitymap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

#include "am/zdbam.h"
#include "util/curl_support.h"
#include "rest/rest.h"
#include "util/zdbutils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(rest_get);
PG_FUNCTION_INFO_V1(zdb_invisible_pages);

Datum rest_get(PG_FUNCTION_ARGS);
Datum zdb_invisible_pages(PG_FUNCTION_ARGS);

static int tuple_is_visible(IndexScanDesc scan);

/*
 * Library initialization functions
 */
void _PG_init(void);
void _PG_fini(void);

void _PG_init(void) {
    curl_support_init();

    zdbam_init();
}

void _PG_fini(void) {
    zdbam_fini();
}

Datum rest_get(PG_FUNCTION_ARGS) {
    char *url = PG_ARGISNULL(0) ? NULL : GET_STR(PG_GETARG_TEXT_P(0));

    if (url == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(cstring_to_text(rest_call("GET", url, NULL)->data));
}

Datum zdb_invisible_pages(PG_FUNCTION_ARGS) {
    Oid        relid = PG_GETARG_OID(0);
    Relation   rel;
    StringInfo sb    = makeStringInfo();

    rel = RelationIdGetRelation(relid);
    if (visibilitymap_count(rel) != rel->rd_rel->relpages) {
        BlockNumber       i;
        OffsetNumber      j;
        IndexScanDescData scan;

        memset(&scan, 0, sizeof(IndexScanDescData));
        scan.heapRelation = rel;
        scan.xs_snapshot  = GetActiveSnapshot();

        for (i = 0; i < RelationGetNumberOfBlocks(rel); i++) {
            Buffer vmap_buff = InvalidBuffer;
            bool   allVisible;

            allVisible = visibilitymap_test(rel, i, &vmap_buff);
            if (!BufferIsInvalid(vmap_buff)) {
                ReleaseBuffer(vmap_buff);
                vmap_buff = InvalidBuffer;
            }

            if (!allVisible) {
                for (j = 1; j <= MaxOffsetNumber; j++) {
                    int rc;

                    ItemPointerSet(&(scan.xs_ctup.t_self), i, j);
                    rc = tuple_is_visible(&scan);
                    if (rc == 2)
                        break; /* we're done with this page */

                    if (rc == 0) {
                        /* tuple is invisible to us */
                        if (sb->len > 0) appendStringInfoChar(sb, ' ');
                        appendStringInfo(sb, "(%d,%d)", i, j);
                    }
                }
                if (BufferIsValid(scan.xs_cbuf)) {
                    ReleaseBuffer(scan.xs_cbuf);
                    scan.xs_cbuf = InvalidBuffer;
                }
            }
        }
    }
    RelationClose(rel);

    PG_RETURN_TEXT_P(cstring_to_text(sb->data));
}

static int tuple_is_visible(IndexScanDesc scan) {
    ItemPointer tid      = &scan->xs_ctup.t_self;
    bool        all_dead = false;
    bool        got_heap_tuple;
    Page        page;

    if (scan->xs_cbuf == InvalidBuffer) {
        /* Switch to correct buffer if we don't have it already */
        scan->xs_cbuf = ReleaseAndReadBuffer(scan->xs_cbuf, scan->heapRelation, ItemPointerGetBlockNumber(tid));
    }

    page = BufferGetPage(scan->xs_cbuf);
    if (ItemPointerGetOffsetNumber(tid) > PageGetMaxOffsetNumber(page)) {
        /* no more items on this page */
        return 2;
    }

    /* Obtain share-lock on the buffer so we can examine visibility */
    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
    got_heap_tuple = heap_hot_search_buffer(tid, scan->heapRelation, scan->xs_cbuf, scan->xs_snapshot, &scan->xs_ctup, &all_dead, !scan->xs_continue_hot);
    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);

    return got_heap_tuple;
}
