/**
 * Copyright 2018 ZomboDB, LLC
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

#ifndef __ZDB_ZDBQUERYTYPE_H__
#define __ZDB_ZDBQUERYTYPE_H__

#include "postgres.h"
#include "fmgr.h"

typedef struct ZDBQueryType {
	int32 vl_len_;        /* varlena header (do not touch directly!) */
	char  json[FLEXIBLE_ARRAY_MEMBER];
	/* query string follows */
} ZDBQueryType;

ZDBQueryType *zdbquery_in_direct(char *input);

bool zdbquery_get_wants_score(ZDBQueryType *query);
uint64 zdbquery_get_row_estimate(ZDBQueryType *query);
uint64 zdbquery_get_limit(ZDBQueryType *query);
uint64 zdbquery_get_offset(ZDBQueryType *query);
char *zdbquery_get_sort_field(ZDBQueryType *query);
char *zdbquery_get_sort_direction(ZDBQueryType *query);
double zdbquery_get_min_score(ZDBQueryType *query);
char *zdbquery_get_query(ZDBQueryType *query);

#define MakeZDBQuery(cstr) zdbquery_in_direct(cstr)


#endif /* __ZDB_ZDBQUERYTYPE_H__ */
