/**
 * Copyright 2020 ZomboDB, LLC
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

#include "type_conversion.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/json.h"

/* copied from json_pg11.c */
typedef enum                    /* type categories for datum_to_json */
{
    JSONTYPE_NULL,                /* null, so we didn't bother to identify */
    JSONTYPE_BOOL,                /* boolean (built-in types only) */
    JSONTYPE_NUMERIC,            /* numeric (ditto) */
    JSONTYPE_DATE,                /* we use special formatting for datetimes */
    JSONTYPE_TIMESTAMP,
    JSONTYPE_TIMESTAMPTZ,
    JSONTYPE_JSON,                /* JSON itself (and JSONB) */
    JSONTYPE_ARRAY,                /* array */
    JSONTYPE_COMPOSITE,            /* composite */
    JSONTYPE_CAST,                /* something with an explicit cast to JSON */
    JSONTYPE_OTHER                /* all else */
} JsonTypeCategory;

extern void datum_to_json(Datum val, bool is_null, StringInfo result, JsonTypeCategory tcategory, Oid outfuncoid,
                          bool key_scalar);

extern void json_categorize_type(Oid typoid, JsonTypeCategory *tcategory, Oid *outfuncoid);

static Oid lookup_json_converter(Oid typeoid) {
    static Oid  argtypes[] = {REGPROCOID};
    static char *nulls     = "\0";
    Datum       args       = ObjectIdGetDatum(typeoid);
    int         res;
    Oid         funcoid;

    SPI_connect();

    if ((res = SPI_execute_with_args("select funcoid from zdb.type_conversions where typeoid = $1;",
                                     1, argtypes, &args, nulls, true, 1) != SPI_OK_SELECT))
        elog(ERROR, "Problem looking up json type converter for type=%u, result=%d", typeoid, res);

    if (SPI_processed > 0) {
        bool isnull;

        funcoid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    } else {
        funcoid = InvalidOid;
    }

    SPI_finish();

    return funcoid;
}

JsonConversion **build_json_conversions(TupleDesc tupdesc) {
    JsonConversion **conversions = palloc(sizeof(JsonConversion *) * tupdesc->natts);
    int            i;

    for (i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        conversions[i] = palloc(sizeof(JsonConversion));
        conversions[i]->funcoid = lookup_json_converter(attr->atttypid);
    }

    return conversions;
}

/**
 * Similar to Postgres' "json.c#composite_to_json()" function, but modified a bit to use a list
 * of "JsonConversion" functions to do custom conversions for ZDB
 */
void zdb_row_to_json(StringInfo json, Datum row, TupleDesc tupdesc, JsonConversion **conversions) {
    HeapTupleHeader td;
    HeapTupleData   tmptup;
    HeapTupleData   *tuple;
    int             i;

    td = DatumGetHeapTupleHeader(row);

    /* Build a temporary HeapTuple control structure */
    tmptup.t_len  = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple = &tmptup;

    appendStringInfoChar(json, '{');

    for (i = 0; i < tupdesc->natts; i++) {
        Datum             val;
        bool isnull;
        char              *attname;
        JsonTypeCategory  tcategory;
        Oid               outfuncoid;
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        if (att->attisdropped)
            continue;

        if (json->len > 1)
            appendStringInfoChar(json, ',');

        attname = NameStr(att->attname);
        escape_json(json, attname);
        appendStringInfoChar(json, ':');

        val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

        if (isnull) {
            appendStringInfoString(json, "null");
        } else if (conversions[i]->funcoid == InvalidOid) {
            json_categorize_type(att->atttypid,
                                 &tcategory, &outfuncoid);
            datum_to_json(val, isnull, json, tcategory, outfuncoid, false);
        } else {
            Datum conv = OidFunctionCall2(conversions[i]->funcoid, val, Int32GetDatum(att->atttypmod));
            appendStringInfoString(json, TextDatumGetCString(conv));
        }
    }

    appendStringInfoChar(json, '}');
}
