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
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/visibilitymap.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "zdbutils.h"

static int tuple_is_visible(IndexScanDesc scan);

char *lookup_analysis_thing(MemoryContext cxt, char *thing) {
    char *definition = "";
    StringInfo query;

    SPI_connect();

    query = makeStringInfo();
    appendStringInfo(query, "select (to_json(name) || ':' || definition) from %s;", TextDatumGetCString(DirectFunctionCall1(quote_ident, CStringGetTextDatum(thing))));

    if (SPI_execute(query->data, true, 0) != SPI_OK_SELECT)
        elog(ERROR, "Problem looking up analysis thing with query: %s", query->data);

    if (SPI_processed > 0) {
        StringInfo json = makeStringInfo();
        int i;

        for (i=0; i<SPI_processed; i++) {
            if (i>0) appendStringInfoCharMacro(json, ',');
            appendStringInfo(json, "%s", SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
        }
        definition = (char *) MemoryContextAllocZero(cxt, (Size) json->len + 1);
        memcpy(definition, json->data, json->len);
    }

    SPI_finish();

    return definition;
}

char *lookup_field_mapping(MemoryContext cxt, Oid tableRelId, char *fieldname) {
    char *definition = NULL;
    StringInfo query;

    SPI_connect();

    query = makeStringInfo();
    appendStringInfo(query, "select definition from zdb_mappings where table_name = %d::regclass and field_name = %s;", tableRelId, TextDatumGetCString(DirectFunctionCall1(quote_literal, CStringGetTextDatum(fieldname))));

    if (SPI_execute(query->data, true, 2) != SPI_OK_SELECT)
        elog(ERROR, "Problem looking up analysis thing with query: %s", query->data);

    if (SPI_processed > 1) {
        elog(ERROR, "Too many mappings found");
    } else if (SPI_processed == 1) {
        char *json = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        Size len = strlen(json);

        definition = (char *) MemoryContextAllocZero(cxt, (Size) len + 1);
        memcpy(definition, json, len);
    }

    SPI_finish();

    return definition;
}

bool type_is_domain(char *type_name, Oid *base_type) {
    bool rc;
    StringInfo query;

    SPI_connect();
    query = makeStringInfo();
    appendStringInfo(query, "SELECT typtype = 'd', typbasetype FROM pg_type WHERE typname = %s", TextDatumGetCString(DirectFunctionCall1(quote_literal, CStringGetTextDatum(type_name))));

    if (SPI_execute(query->data, true, 1) != SPI_OK_SELECT)
        elog(ERROR, "Problem determing if %s is a domain with query: %s", type_name, query->data);

    if (SPI_processed == 0) {
        rc = false;
    } else {
        bool isnull;
        Datum d;

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        rc = isnull || DatumGetBool(d);

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        *base_type = isnull ? InvalidOid : DatumGetObjectId(d);
    }

    SPI_finish();

    return rc;
}


void appendBinaryStringInfoAndStripLineBreaks(StringInfo str, const char *data, int datalen)
{
    int i;
    Assert(str != NULL);

    /* Make more room if needed */
    enlargeStringInfo(str, datalen);

    /* OK, append the data */
    memcpy(str->data + str->len, data, datalen);
    for (i=str->len; i<str->len+datalen; i++) {
        switch (str->data[i]) {
            case '\r':
            case '\n':
                str->data[i] = ' ';
        }
    }
    str->len += datalen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data.  (Some callers are dealing with text but call this because
     * their input isn't null-terminated.)
     */
    str->data[str->len] = '\0';
}


void freeStringInfo(StringInfo si) {
    if (si != NULL) {
        pfree(si->data);
        pfree(si);
    }
}

char *lookup_primary_key(char *schemaName, char *tableName, bool failOnMissing)
{
    StringInfo sql = makeStringInfo();
    char *keyname;

    SPI_connect();
    appendStringInfo(sql, "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName);
    SPI_execute(sql->data, true, 1);

    if (SPI_processed == 0)
    {
        if (failOnMissing)
            elog(ERROR, "Cannot find primary key column for: %s.%s", schemaName, tableName);
        else
        {
            SPI_finish();
            return NULL;
        }
    }

    keyname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    if (keyname == NULL)
        elog(ERROR, "Primary Key field is null for: %s.%s", schemaName, tableName);

    keyname = MemoryContextStrdup(TopTransactionContext, keyname);

    SPI_finish();

    return keyname;
}

Oid *findZDBIndexes(Oid relid, int *many) {
    Oid        *indexes = NULL;
    StringInfo sql;
    int        i;

    SPI_connect();

    sql = makeStringInfo();
    appendStringInfo(sql, "select indexrelid "
            "from pg_index "
            "where indrelid = %d "
            "  and indclass[0] = (select oid from pg_opclass where opcmethod = (select oid from pg_am where amname = 'zombodb') and opcname = 'zombodb_tid_ops')", relid);

    SPI_execute(sql->data, true, 1);
    *many = SPI_processed;
    if (SPI_processed > 0) {
        indexes = (Oid *) MemoryContextAlloc(TopTransactionContext, sizeof(Oid) * SPI_processed);
        for (i = 0; i < SPI_processed; i++)
            indexes[i] = (Oid) atoi(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
    }
    SPI_finish();

    return indexes;
}

Oid *oid_array_to_oids(ArrayType *arr, int *many)
{
    if (ARR_NDIM(arr) != 1 ||
        ARR_HASNULL(arr) ||
        ARR_ELEMTYPE(arr) != OIDOID)
        elog(ERROR, "expected oid[] of non-null values");
    *many = ARR_DIMS(arr)[0];
    return (Oid *) ARR_DATA_PTR(arr);
}

char **text_array_to_strings(ArrayType *array, int *many)
{
    char  **result;
    Datum *elements;
    int   nelements;
    int   i;

    Assert(ARR_ELEMTYPE(array) == TEXTOID);

    deconstruct_array(array, TEXTOID, -1, false, 'i',
                      &elements, NULL, &nelements);

    result = (char **) palloc(nelements * (sizeof (char *)));
    for (i = 0; i < nelements; i++)
    {
        result[i] = TextDatumGetCString(elements[i]);
        if (result[i] == NULL)
            elog(ERROR, "expected text[] of non-null values");
    }

    *many = nelements;
    return result;
}

static void string_invisibility_callback(ItemPointer ctid, void *stringInfo) {
    StringInfo sb = (StringInfo) stringInfo;
    if (sb->len > 0)
        appendStringInfoChar(sb, ',');
    appendStringInfo(sb, "%d-%d", ItemPointerGetBlockNumber(ctid), ItemPointerGetOffsetNumber(ctid));
}

StringInfo find_invisible_ctids(Relation rel) {
    StringInfo sb = makeStringInfo();

    find_invisible_ctids_with_callback(rel, string_invisibility_callback, sb);
    return sb;
}

int find_invisible_ctids_with_callback(Relation heapRel, invisibility_callback cb, void *user_data) {
    int cnt = visibilitymap_count(heapRel);
    int many = 0;

    if (cnt == 0 || cnt != heapRel->rd_rel->relpages) {
        BlockNumber       i;
        OffsetNumber      j;
        IndexScanDescData scan;

        memset(&scan, 0, sizeof(IndexScanDescData));
        scan.heapRelation = heapRel;
        scan.xs_snapshot  = GetActiveSnapshot();

        for (i = 0; i < RelationGetNumberOfBlocks(heapRel); i++) {
            Buffer vmap_buff = InvalidBuffer;
            bool   allVisible;

            allVisible = visibilitymap_test(heapRel, i, &vmap_buff);
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
                        cb(&scan.xs_ctup.t_self, user_data);
                        many++;
                    }
                }
                if (BufferIsValid(scan.xs_cbuf)) {
                    ReleaseBuffer(scan.xs_cbuf);
                    scan.xs_cbuf = InvalidBuffer;
                }
            }
        }
    }

    return many;
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
