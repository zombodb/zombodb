/**
 * Copyright 2018-2019 ZomboDB, LLC
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

#ifndef __ZDB_ZDBAM_H__
#define __ZDB_ZDBAM_H__

#include "zombodb.h"
#include "elasticsearch/elasticsearch.h"
#include "zdb_index_options.h"
#include "utils/guc.h"

typedef struct ZDBIndexChangeContext {
	Oid                      indexRelid;
	ElasticsearchBulkContext *esContext;
	MemoryContext            scratch;
} ZDBIndexChangeContext;


ZDBIndexChangeContext *checkout_insert_context(Relation indexRelation, Datum row, bool isnull);
void finish_inserts(bool is_commit);
Datum collect_used_xids(MemoryContext memoryContext);

#endif /* __ZDB_ZDBAM_H__ */
