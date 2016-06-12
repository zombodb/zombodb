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

#include "miscadmin.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

#include "zdbutils.h"

static int find_invisible_ctids_with_callback(Relation heapRel, Relation xactRel, invisibility_callback cb, void *ctids, void *xids);

typedef struct {
    TransactionId last_xid;
    uint32        epoch;
} TxidEpoch;

char *lookup_analysis_thing(MemoryContext cxt, char *thing) {
    char       *definition = "";
    StringInfo query;

    SPI_connect();

    query = makeStringInfo();
    appendStringInfo(query, "select (to_json(name) || ':' || definition) from %s;", TextDatumGetCString(DirectFunctionCall1(quote_ident, CStringGetTextDatum(thing))));

    if (SPI_execute(query->data, true, 0) != SPI_OK_SELECT)
        elog(ERROR, "Problem looking up analysis thing with query: %s", query->data);

    if (SPI_processed > 0) {
        StringInfo json = makeStringInfo();
        int        i;

        for (i = 0; i < SPI_processed; i++) {
            if (i > 0) appendStringInfoCharMacro(json, ',');
            appendStringInfo(json, "%s", SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
        }
        definition = (char *) MemoryContextAllocZero(cxt, (Size) json->len + 1);
        memcpy(definition, json->data, json->len);
    }

    SPI_finish();

    return definition;
}

char *lookup_field_mapping(MemoryContext cxt, Oid tableRelId, char *fieldname) {
    char       *definition = NULL;
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
        Size len   = strlen(json);

        definition = (char *) MemoryContextAllocZero(cxt, (Size) len + 1);
        memcpy(definition, json, len);
    }

    SPI_finish();

    return definition;
}

bool type_is_domain(char *type_name, Oid *base_type) {
    bool       rc;
    StringInfo query;

    SPI_connect();
    query = makeStringInfo();
    appendStringInfo(query, "SELECT typtype = 'd', typbasetype FROM pg_type WHERE typname = %s", TextDatumGetCString(DirectFunctionCall1(quote_literal, CStringGetTextDatum(type_name))));

    if (SPI_execute(query->data, true, 1) != SPI_OK_SELECT)
        elog(ERROR, "Problem determing if %s is a domain with query: %s", type_name, query->data);

    if (SPI_processed == 0) {
        rc = false;
    } else {
        bool  isnull;
        Datum d;

        d  = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        rc = isnull || DatumGetBool(d);

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        *base_type = isnull ? InvalidOid : DatumGetObjectId(d);
    }

    SPI_finish();

    return rc;
}


void appendBinaryStringInfoAndStripLineBreaks(StringInfo str, const char *data, int datalen) {
    int i;
    Assert(str != NULL);

    /* Make more room if needed */
    enlargeStringInfo(str, datalen+1);

    /* OK, append the data */
    for (i=0; i<datalen; i++) {
        char ch = data[i];
        switch (ch) {
            case '\r':
            case '\n':
                appendStringInfoCharMacro(str, ' ');
                break;
            default:
                appendStringInfoCharMacro(str, ch);
                break;
        }
    }
}


void freeStringInfo(StringInfo si) {
    if (si != NULL) {
        pfree(si->data);
        pfree(si);
    }
}

char *lookup_primary_key(char *schemaName, char *tableName, bool failOnMissing) {
    StringInfo sql = makeStringInfo();
    char       *keyname;

    SPI_connect();
    appendStringInfo(sql, "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName);
    SPI_execute(sql->data, true, 1);

    if (SPI_processed == 0) {
        if (failOnMissing)
            elog(ERROR, "Cannot find primary key column for: %s.%s", schemaName, tableName);
        else {
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
        for (i  = 0; i < SPI_processed; i++)
            indexes[i] = (Oid) atoi(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
    }
    SPI_finish();

    return indexes;
}

Oid *oid_array_to_oids(ArrayType *arr, int *many) {
    if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID)
        elog(ERROR, "expected oid[] of non-null values");
    *many = ARR_DIMS(arr)[0];
    return (Oid *) ARR_DATA_PTR(arr);
}

char **text_array_to_strings(ArrayType *array, int *many) {
    char  **result;
    Datum *elements;
    int   nelements;
    int   i;

    Assert(ARR_ELEMTYPE(array) == TEXTOID);

    deconstruct_array(array, TEXTOID, -1, false, 'i', &elements, NULL, &nelements);

    result = (char **) palloc(nelements * (sizeof(char *)));
    for (i = 0; i < nelements; i++) {
        result[i] = TextDatumGetCString(elements[i]);
        if (result[i] == NULL)
            elog(ERROR, "expected text[] of non-null values");
    }

    *many = nelements;
    return result;
}

Oid get_relation_oid(char *relname) {
    RangeVar   *rv;

    rv = makeRangeVarFromNameList(textToQualifiedNameList(cstring_to_text(relname)));

    /* We might not even have permissions on this relation; don't lock it. */
    return RangeVarGetRelid(rv, NoLock, true);
}

static void string_invisibility_callback(ItemPointer ctid, uint64 xid, void *ctids, void *xids) {

    if (ctid != NULL) {
        StringInfo sb = (StringInfo) ctids;
        if (sb->len > 0)
            appendStringInfoCharMacro(sb, ',');
        appendStringInfo(sb, "%lu", ItemPointerToUint64(ctid));
    }

    if (xid != InvalidTransactionId) {
        StringInfo sb = (StringInfo) xids;
        if (sb->len > 0)
            appendStringInfoCharMacro(sb, ',');
        appendStringInfo(sb, "%lu", xid);
    }

}

StringInfo find_invisible_ctids(Relation heapRel, Relation xactRel, StringInfo ctids, StringInfo xids) {
    find_invisible_ctids_with_callback(heapRel, xactRel, string_invisibility_callback, ctids, xids);
    return ctids;
}

bool is_active_xid(Snapshot snapshot, TransactionId xid) {
    int i;

    if (xid >= snapshot->xmax)
        return true;

    for (i=0; i<snapshot->xcnt; i++) {
        if (snapshot->xip[i] == xid)
            return true;
    }
    return false;
}

static int find_invisible_ctids_with_callback(Relation heapRel, Relation xactRel, invisibility_callback cb, void *ctids, void *xids) {
    Snapshot       snapshot   = GetActiveSnapshot();
    TransactionId  currentxid = GetCurrentTransactionId();
    TransactionId  lastxid    = InvalidTransactionId;
    IndexScanDesc  scanDesc;
    ItemPointer    tid;
    int            invs       = 0, current = 0, skipped = 0, aborted = 0, tuples = 0;
    struct timeval tv1, tv2;

    gettimeofday(&tv1, NULL);

    if (visibilitymap_count(heapRel) != RelationGetNumberOfBlocks(heapRel)) {
        scanDesc = index_beginscan(heapRel, xactRel, SnapshotAny, 0, 0);
        scanDesc->xs_want_itup = true;
        index_rescan(scanDesc, NULL, 0, NULL, 0);

        while ((tid = index_getnext_tid(scanDesc, ForwardScanDirection)) != NULL) {
            uint64        convertedxid;
            TransactionId xid;
            bool          is_insert;
            bool          isnull;

            convertedxid = (uint64) DatumGetInt64(index_getattr(scanDesc->xs_itup, 1, scanDesc->xs_itupdesc, &isnull));
            is_insert    = DatumGetBool(index_getattr(scanDesc->xs_itup, 2, scanDesc->xs_itupdesc, &isnull));
            xid          = (TransactionId) (convertedxid);

            //
            // if xid is current
            //      then add tid to list of ctids we should exclude
            // else if xid is currently active
            //      then skip it entirely
            // else if xid aborted
            //      then add xid to list of xids we should exclude
            // else if not is_insert and xid committed
            //      then add tid to list of ctids we should exclude
            //

            if (currentxid == xid) {
                if (!is_insert) {
                    cb(tid, InvalidTransactionId, ctids, xids);
                }
                current++;
            } else if (is_active_xid(snapshot, xid)) {
                /* xact is still active, so skip because these are wholesale excluded in our query */
                skipped++;
            } else if (TransactionIdDidAbort(xid)) {
                if (lastxid != xid) {
                    cb(NULL, convertedxid, ctids, xids);
                    aborted++;
                    lastxid = xid;
                }
            } else if (TransactionIdDidCommit(xid)) {
                /* ignore ctid of committed update/delete */
                if (!is_insert) {
                    cb(tid, InvalidTransactionId, ctids, xids);
                    invs++;
                }
            }

            tuples++;
        }

        index_endscan(scanDesc);
    }

    gettimeofday(&tv2, NULL);
    elog(LOG, "[zombodb invisibility stats] heap=%s, invisible=%d, skipped=%d, current=%d, aborted=%d, tuples=%d, ttl=%fs", RelationGetRelationName(heapRel), invs, skipped, current, aborted, tuples, TO_SECONDS(tv1, tv2));

    return invs;
}

/* adapted from Postgres' txid.c#convert_xid function */
uint64 convert_xid(TransactionId xid) {
    TxidEpoch state;
    uint64    epoch;

    GetNextXidAndEpoch(&state.last_xid, &state.epoch);

    /* return special xid's as-is */
    if (!TransactionIdIsNormal(xid))
        return (uint64) xid;

    /* xid can be on either side when near wrap-around */
    epoch = (uint64) state.epoch;
    if (xid > state.last_xid && TransactionIdPrecedes(xid, state.last_xid))
        epoch--;
    else if (xid < state.last_xid && TransactionIdFollows(xid, state.last_xid))
        epoch++;

    return (epoch << 32) | xid;
}