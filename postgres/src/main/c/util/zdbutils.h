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
#ifndef ZDBUTILS_H
#define ZDBUTILS_H

#include "postgres.h"
#include "catalog/dependency.h"
#include "lib/stringinfo.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/snapshot.h"

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))
#define ItemPointerToUint64(ht_ctid) (((uint64)ItemPointerGetBlockNumber(ht_ctid)) << 32) | ((uint32)ItemPointerGetOffsetNumber(ht_ctid))
#define TO_SECONDS(tv1, tv2) ((double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec))

#define zdb_json char *

char *lookup_analysis_thing(MemoryContext cxt, char *thing);
char *lookup_field_mapping(MemoryContext cxt, Oid tableRelId, char *fieldname);
bool type_is_domain(char *type_name, Oid *base_type);
void appendBinaryStringInfoAndStripLineBreaks(StringInfo str, const char *data, int datalen);
void freeStringInfo(StringInfo si);
char *lookup_primary_key(char *schemaName, char *tableName, bool failOnMissing);
Oid  *findZDBIndexes(Oid relid, int *many);
Oid *find_all_zdb_indexes(int *many);
Oid  *oid_array_to_oids(ArrayType *arr, int *many);
char **text_array_to_strings(ArrayType *array, int *many);

uint64     convert_xid(TransactionId xid);

Expr *evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod, Oid result_collation);
uint64 lookup_pkey(Oid heapRelOid, char *pkeyFieldname, ItemPointer ctid);

#endif
