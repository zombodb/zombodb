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
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/memutils.h"

#include "zdbutils.h"

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
    enlargeStringInfo(str, datalen);

    /* OK, append the data */
    memcpy(str->data + str->len, data, datalen);
    for (i = str->len; i < str->len + datalen; i++) {
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

