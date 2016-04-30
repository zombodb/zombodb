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
#include "lib/stringinfo.h"
#include "utils/array.h"

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

#define zdb_json char *

extern PGDLLEXPORT uint64 ConvertedSnapshotXmax;
extern PGDLLEXPORT uint64 ConvertedTopTransactionId;

char *lookup_analysis_thing(MemoryContext cxt, char *thing);
char *lookup_field_mapping(MemoryContext cxt, Oid tableRelId, char *fieldname);
bool type_is_domain(char *type_name, Oid *base_type);
void appendBinaryStringInfoAndStripLineBreaks(StringInfo str, const char *data, int datalen);
void freeStringInfo(StringInfo si);
char *lookup_primary_key(char *schemaName, char *tableName, bool failOnMissing);
Oid *findZDBIndexes(Oid relid, int *many);
Oid  *oid_array_to_oids(ArrayType *arr, int *many);
char **text_array_to_strings(ArrayType *array, int *many);

typedef void (*invisibility_callback)(ItemPointer ctid, void *data);
int        find_invisible_ctids_with_callback(Relation heapRel, bool isVacuum, invisibility_callback cb, void *user_data);
StringInfo find_invisible_ctids(Relation rel);
uint64 convert_xid(TransactionId xid);

#endif
