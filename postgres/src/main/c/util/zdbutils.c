/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2017 ZomboDB, LLC
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
#include "access/xlog.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "optimizer/planmain.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

#include "am/zdb_interface.h"

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

Oid *find_all_zdb_indexes(int *many) {
    Oid        *indexes = NULL;
    int        i;

    SPI_connect();

    SPI_execute("select oid from pg_class where relam = (select oid from pg_am where amname = 'zombodb');", true, 0);
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

/*
 * Copied from Postgres' src/backend/optimizer/util/clauses.c
 *
 * evaluate_expr: pre-evaluate a constant expression
 *
 * We use the executor's routine ExecEvalExpr() to avoid duplication of
 * code and ensure we get the same result as the executor would get.
 */
Expr *
evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
              Oid result_collation)
{
    EState	   *estate;
    ExprState  *exprstate;
    MemoryContext oldcontext;
    Datum		const_val;
    bool		const_is_null;
    int16		resultTypLen;
    bool		resultTypByVal;

    /*
     * To use the executor, we need an EState.
     */
    estate = CreateExecutorState();

    /* We can use the estate's working context to avoid memory leaks. */
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    /* Make sure any opfuncids are filled in. */
    fix_opfuncids((Node *) expr);

    /*
     * Prepare expr for execution.  (Note: we can't use ExecPrepareExpr
     * because it'd result in recursively invoking eval_const_expressions.)
     */
    exprstate = ExecInitExpr(expr, NULL);

    /*
     * And evaluate it.
     *
     * It is OK to use a default econtext because none of the ExecEvalExpr()
     * code used in this situation will use econtext.  That might seem
     * fortuitous, but it's not so unreasonable --- a constant expression does
     * not depend on context, by definition, n'est ce pas?
     */
    const_val = ExecEvalExprSwitchContext(exprstate,
                                          GetPerTupleExprContext(estate),
                                          &const_is_null, NULL);

    /* Get info needed about result datatype */
    get_typlenbyval(result_type, &resultTypLen, &resultTypByVal);

    /* Get back to outer memory context */
    MemoryContextSwitchTo(oldcontext);

    /*
     * Must copy result out of sub-context used by expression eval.
     *
     * Also, if it's varlena, forcibly detoast it.  This protects us against
     * storing TOAST pointers into plans that might outlive the referenced
     * data.  (makeConst would handle detoasting anyway, but it's worth a few
     * extra lines here so that we can do the copy and detoast in one step.)
     */
    if (!const_is_null)
    {
        if (resultTypLen == -1)
            const_val = PointerGetDatum(PG_DETOAST_DATUM_COPY(const_val));
        else
            const_val = datumCopy(const_val, resultTypByVal, resultTypLen);
    }

    /* Release all the junk we just created */
    FreeExecutorState(estate);

    /*
     * Make the constant result node.
     */
    return (Expr *) makeConst(result_type, result_typmod, result_collation,
                              resultTypLen,
                              const_val, const_is_null,
                              resultTypByVal);
}

uint64 lookup_pkey(Oid heapRelOid, char *pkeyFieldname, ItemPointer ctid) {
    Relation heapRel;
    HeapTupleData tuple;
    Buffer buffer;
    Datum pkey;
    bool isnull;

    heapRel = RelationIdGetRelation(heapRelOid);
    tuple.t_self = *ctid;

    if (!heap_fetch(heapRel, SnapshotAny, &tuple, &buffer, false, NULL))
        elog(ERROR, "Unable to fetch heap page from index");

    pkey = heap_getattr(&tuple, get_attnum(heapRelOid, pkeyFieldname), RelationGetDescr(heapRel), &isnull);
    if (isnull)
        elog(ERROR, "detected NULL key value.  Cannot update row");

    ReleaseBuffer(buffer);
    RelationClose(heapRel);

    return DatumGetInt64(pkey);
}

Oid create_trigger(char *zombodbNamespace, char *schemaname, char *relname, Oid relid, char *triggerName, char *functionName, Oid arg, int16 type) {
	CreateTrigStmt *tgstmt;
	RangeVar       *relrv = makeRangeVar(schemaname, relname, -1);
	Oid            triggerOid;
	List           *args  = NIL;

    if (arg != InvalidOid) {
        StringInfo arg_str = makeStringInfo();
        appendStringInfo(arg_str, "%u", arg);
        args = lappend(NIL, makeString(pstrdup(arg_str->data)));
        freeStringInfo(arg_str);
    }

    tgstmt = makeNode(CreateTrigStmt);
    tgstmt->trigname     = triggerName;
    tgstmt->relation     = copyObject(relrv);
    tgstmt->funcname     = list_make2(makeString(zombodbNamespace), makeString(functionName));
    tgstmt->args         = args;
    tgstmt->row          = true;
    tgstmt->timing       = TRIGGER_TYPE_BEFORE;
    tgstmt->events       = type;
    tgstmt->columns      = NIL;
    tgstmt->whenClause   = NULL;
    tgstmt->isconstraint = false;
    tgstmt->deferrable   = false;
    tgstmt->initdeferred = false;
    tgstmt->constrrel    = NULL;

    triggerOid = CreateTrigger(tgstmt, NULL, relid, InvalidOid, InvalidOid, InvalidOid, true /* tgisinternal */);

    /* Make the new trigger visible within this session */
    CommandCounterIncrement();

	return triggerOid;
}
