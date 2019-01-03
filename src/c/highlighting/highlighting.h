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

#ifndef __ZDB_HIGHLIGHTING_H__
#define __ZDB_HIGHLIGHTING_H__

#include "zombodb.h"
#include "json/json_support.h"

typedef struct ZDBHighlightInfo {
	char *name;
	char *json;
} ZDBHighlightInfo;

#define HIGHLIGHT_FIELD_MAX_LENGTH 8192
typedef struct ZDBHighlightFieldnameData {
	char data[HIGHLIGHT_FIELD_MAX_LENGTH];
} ZDBHighlightFieldnameData;

typedef struct ZDBHighlightKey {
	ItemPointerData ctid;
	ZDBHighlightFieldnameData field;
} ZDBHighlightKey;

typedef struct ZDBHighlightEntry {
	ZDBHighlightKey key;
	List            *highlights;
} ZDBHighlightEntry;

typedef List *(*highlight_lookup_callback)(ItemPointer ctid, ZDBHighlightFieldnameData *field, void *arg);

void highlight_support_init(void);
void highlight_support_cleanup(void);
List *extract_highlight_info(IndexScanDesc scan, Oid healRelid);
void save_highlights(HTAB *hash, ItemPointer ctid, zdb_json_object highlights);
HTAB *highlight_create_lookup_table(MemoryContext memoryContext, char *name);
void highlight_register_callback(Oid heapOid, highlight_lookup_callback callback, void *callback_data, MemoryContext memoryContext);

#endif /* __ZDB_HIGHLIGHTING_H__ */
