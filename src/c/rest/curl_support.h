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

#ifndef __ZDB_CURL_SUPPORT_H__
#define __ZDB_CURL_SUPPORT_H__

#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

#include <curl/curl.h>

/* this needs to match elasticsearch.h:MAX_BULK_CONCURRENCY */
#define MAX_CURL_HANDLES 1024

typedef struct PostDataEntry {
	int        pool_idx;
	StringInfo buff;
	char       *compressed_data;
} PostDataEntry;

typedef struct MultiRestState {
	int               nhandles;
	CURL              *handles[MAX_CURL_HANDLES];
	struct curl_slist *headers[MAX_CURL_HANDLES];
	char              *errorbuffs[MAX_CURL_HANDLES];
	PostDataEntry     *postDatas[MAX_CURL_HANDLES];
	StringInfo        responses[MAX_CURL_HANDLES];
	bool              vconflicts[MAX_CURL_HANDLES];    /* should we ignore version conflicts for this request? */

	CURLM *multi_handle;
	int   available;

	StringInfo *pool;
} MultiRestState;

extern CURL *GLOBAL_CURL_INSTANCE;
extern char GLOBAL_CURL_ERRBUF[CURL_ERROR_SIZE];

void curl_support_init(void);
void curl_record_multi_handle(MultiRestState *state);
void curl_forget_multi_handle(MultiRestState *state);

#endif /* __ZDB_CURL_SUPPORT_H__ */
