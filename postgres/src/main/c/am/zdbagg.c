/*
 * Copyright 2013-2015 Technology Concepts & Design, Inc
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
#include "access/xact.h"
#include "utils/builtins.h"

#include "am/zdbagg.h"
#include "am/zdb_interface.h"
#include "util/zdbutils.h"

PG_FUNCTION_INFO_V1(zdb_internal_actual_index_record_count);
PG_FUNCTION_INFO_V1(zdb_internal_estimate_count);
PG_FUNCTION_INFO_V1(zdb_internal_tally);
PG_FUNCTION_INFO_V1(zdb_internal_range_agg);
PG_FUNCTION_INFO_V1(zdb_internal_significant_terms);
PG_FUNCTION_INFO_V1(zdb_internal_extended_stats);
PG_FUNCTION_INFO_V1(zdb_internal_arbitrary_aggregate);
PG_FUNCTION_INFO_V1(zdb_internal_suggest_terms);
PG_FUNCTION_INFO_V1(zdb_internal_termlist);


Datum zdb_internal_actual_index_record_count(PG_FUNCTION_ARGS) {
    Oid  indexrelid = PG_GETARG_OID(0);
    char *type_name = GET_STR(PG_GETARG_TEXT_P(1));

    ZDBIndexDescriptor *desc;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);

    PG_RETURN_INT64(desc->implementation->actualIndexRecordCount(desc, type_name));
}

Datum zdb_internal_estimate_count(PG_FUNCTION_ARGS) {
    Oid  indexrelid = PG_GETARG_OID(0);
    char *query     = GET_STR(PG_GETARG_TEXT_P(1));

    ZDBIndexDescriptor *desc;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    PG_RETURN_INT64(desc->implementation->estimateCount(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), &query, 1));
}

Datum zdb_internal_tally(PG_FUNCTION_ARGS) {
    Oid   indexrelid  = PG_GETARG_OID(0);
    char  *fieldname  = GET_STR(PG_GETARG_TEXT_P(1));
    char  *stem       = GET_STR(PG_GETARG_TEXT_P(2));
    char  *query      = GET_STR(PG_GETARG_TEXT_P(3));
    int64 max_terms   = PG_GETARG_INT64(4);
    char  *sort_order = GET_STR(PG_GETARG_TEXT_P(5));

    ZDBIndexDescriptor *desc;
    char               *json;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    json = desc->implementation->tally(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), fieldname, stem, query, max_terms, sort_order);

    PG_RETURN_TEXT_P(CStringGetTextDatum(json));
}

Datum zdb_internal_range_agg(PG_FUNCTION_ARGS) {
    Oid  indexrelid  = PG_GETARG_OID(0);
    char *fieldname  = GET_STR(PG_GETARG_TEXT_P(1));
    char *range_spec = GET_STR(PG_GETARG_TEXT_P(2));
    char *query      = GET_STR(PG_GETARG_TEXT_P(3));

    ZDBIndexDescriptor *desc;
    char               *json;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    json = desc->implementation->rangeAggregate(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), fieldname, range_spec, query);

    PG_RETURN_TEXT_P(CStringGetTextDatum(json));
}

Datum zdb_internal_significant_terms(PG_FUNCTION_ARGS) {
    Oid   indexrelid = PG_GETARG_OID(0);
    char  *fieldname = GET_STR(PG_GETARG_TEXT_P(1));
    char  *stem      = GET_STR(PG_GETARG_TEXT_P(2));
    char  *query     = GET_STR(PG_GETARG_TEXT_P(3));
    int64 max_terms  = PG_GETARG_INT64(4);

    ZDBIndexDescriptor *desc;
    char               *json;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    json = desc->implementation->significant_terms(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), fieldname, stem, query, max_terms);

    PG_RETURN_TEXT_P(CStringGetTextDatum(json));
}

Datum zdb_internal_extended_stats(PG_FUNCTION_ARGS) {
    Oid  indexrelid = PG_GETARG_OID(0);
    char *fieldname = GET_STR(PG_GETARG_TEXT_P(1));
    char *query     = GET_STR(PG_GETARG_TEXT_P(2));

    ZDBIndexDescriptor *desc;
    char               *json;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    json = desc->implementation->extended_stats(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), fieldname, query);

    PG_RETURN_TEXT_P(CStringGetTextDatum(json));
}

Datum zdb_internal_arbitrary_aggregate(PG_FUNCTION_ARGS) {
    Oid  indexrelid       = PG_GETARG_OID(0);
    char *aggregate_query = GET_STR(PG_GETARG_TEXT_P(1));
    char *query           = GET_STR(PG_GETARG_TEXT_P(2));

    ZDBIndexDescriptor *desc;
    char               *json;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    json = desc->implementation->arbitrary_aggregate(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), aggregate_query, query);

    PG_RETURN_TEXT_P(CStringGetTextDatum(json));
}

Datum zdb_internal_suggest_terms(PG_FUNCTION_ARGS) {
    Oid   indexrelid = PG_GETARG_OID(0);
    char  *fieldname = GET_STR(PG_GETARG_TEXT_P(1));
    char  *stem      = GET_STR(PG_GETARG_TEXT_P(2));
    char  *query     = GET_STR(PG_GETARG_TEXT_P(3));
    int64 max_terms  = PG_GETARG_INT64(4);

    ZDBIndexDescriptor *desc;
    char               *json;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    json = desc->implementation->suggest_terms(desc, GetCurrentTransactionId(), GetCurrentCommandId(false), fieldname, stem, query, max_terms);

    PG_RETURN_TEXT_P(CStringGetTextDatum(json));
}

Datum zdb_internal_termlist(PG_FUNCTION_ARGS) {
    Oid    indexrelid = PG_GETARG_OID(0);
    char   *fieldname = GET_STR(PG_GETARG_TEXT_P(1));
    char   *prefix    = GET_STR(PG_GETARG_TEXT_P(2));
    char   *startat   = PG_ARGISNULL(3) ? NULL : GET_STR(PG_GETARG_TEXT_P(3));
    uint32 size       = PG_GETARG_UINT32(4);

    ZDBIndexDescriptor *desc;
    char               *json;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);
    json = desc->implementation->termlist(desc, fieldname, prefix, startat, size);

    PG_RETURN_TEXT_P(CStringGetTextDatum(json));
}
