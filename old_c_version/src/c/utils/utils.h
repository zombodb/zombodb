/**
 * Copyright 2018-2020 ZomboDB, LLC
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

#ifndef __ZDB_UTILS_H__
#define __ZDB_UTILS_H__

#include "postgres.h"
#include "access/genam.h"
#include "access/tupdesc.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"

#include "type/zdbquerytype.h"

#define ItemPointerToUint64(ht_ctid) ((((uint64)(ItemPointerGetBlockNumber(ht_ctid))) << 32) | ((uint32)(ItemPointerGetOffsetNumber(ht_ctid))))
#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

#define ZDBFUNC(ns, name, nargs, argtype) LookupFuncName(list_make2(makeString((ns)), makeString((name))), (nargs), argtype, true)
#define ZDBOPER(ns, name, argtype) LookupOperName(NULL, list_make2(makeString((ns)), makeString((name))), TIDOID, (argtype), true, 0)
static inline Oid *oids2(Oid a, Oid b) {
	Oid *rc = palloc(2 * sizeof(Oid));
	rc[0] = a;
	rc[1] = b;
	return rc;
}
static inline Oid *oids3(Oid a, Oid b, Oid c) {
	Oid *rc = palloc(3 * sizeof(Oid));
	rc[0] = a;
	rc[1] = b;
	rc[2] = c;
	return rc;
}

void freeStringInfo(StringInfo si);
Oid get_base_type_oid(Oid typeOid);
TupleDesc lookup_index_tupdesc(Relation indexRelation);
bool tuple_desc_contains_json(TupleDesc tupdesc);
char *text_to_cstring_maybe_no_copy(const text *t, int *len, text **possible_copy);
void replace_line_breaks(char *str, int len, char with_char);
char *strip_json_ending(char *str, int len);
Relation find_zombodb_index(Relation heapRel);
uint64 find_limit_for_scan(IndexScanDesc scan);
uint64 convert_xid(TransactionId xid);
char **array_to_strings(ArrayType *array, int *many);
ZDBQueryType **array_to_zdbqueries(ArrayType *array, int *many);
char *lookup_zdb_namespace(void);
void create_trigger_dependency(Oid indexRelOid, Oid triggerOid);
Oid create_trigger(char *zombodbNamespace, char *schemaname, char *relname, Oid relid, char *triggerName, char *functionName, Oid arg, int16 type);
Relation zdb_open_index(Oid indexRelId, LOCKMODE lock);
TupleDesc extract_tuple_desc_from_index_expressions(IndexInfo *indexInfo);
bool index_is_zdb_index(Relation indexRel);
bool already_has_zdb_index(Relation heapRel, Relation indexRel);
List *lookup_zdb_indexes_in_namespace(Oid namespaceOid);
void set_index_option(Relation rel, char *key, char *value);

#endif /* __ZDB_UTILS_H__ */
