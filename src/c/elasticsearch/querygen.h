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

#ifndef __ZDB_QUERYGEN_H__
#define __ZDB_QUERYGEN_H__

#include "zombodb.h"

char *convert_to_query_dsl_not_wrapped(char *input);
char *convert_to_query_dsl(Relation indexRel, ZDBQueryType *query, bool apply_visibility);
ZDBQueryType *array_to_should_query_dsl(ArrayType *array);
ZDBQueryType *array_to_must_query_dsl(ArrayType *array);
ZDBQueryType *array_to_not_query_dsl(ArrayType *array);
ZDBQueryType *scan_keys_to_query_dsl(ScanKey keys, int nkeys);

#endif /* __ZDB_QUERYGEN_H__ */
