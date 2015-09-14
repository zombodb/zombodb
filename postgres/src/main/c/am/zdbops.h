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
#ifndef __ZDBOPS_H__
#define __ZDBOPS_H__

#include "postgres.h"
#include "fmgr.h"

extern Datum zdb_get_index_name(PG_FUNCTION_ARGS);
extern Datum zdb_get_url(PG_FUNCTION_ARGS);
extern Datum zdb_query_func(PG_FUNCTION_ARGS);
extern Datum zdb_tid_query_func(PG_FUNCTION_ARGS);
extern Datum zdb_table_ref_and_tid(PG_FUNCTION_ARGS);
extern Datum zdb_row_to_json(PG_FUNCTION_ARGS);
extern Datum zdb_internal_describe_nested_object(PG_FUNCTION_ARGS);
extern Datum zdb_internal_get_index_mapping(PG_FUNCTION_ARGS);
extern Datum zdb_internal_highlight(PG_FUNCTION_ARGS);

extern Datum make_es_mapping(TupleDesc tupdesc, bool isAnonymous);

#endif
