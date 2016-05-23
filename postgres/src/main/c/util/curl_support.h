/*
 * Copyright 2015-2016 ZomboDB, LLC
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
#ifndef CURL_SUPPORT_H
#define CURL_SUPPORT_H

#include "postgres.h"
#include "lib/stringinfo.h"

#include <curl/curl.h>

/* this needs to match zdb_interface.h:MAX_BULK_CONCURRENCY */
#define MAX_CURL_HANDLES 1024

typedef struct MultiRestState {
    int        nhandles;
    CURL       *handles[MAX_CURL_HANDLES];
    char       *errorbuffs[MAX_CURL_HANDLES];
    StringInfo postDatas[MAX_CURL_HANDLES];
    StringInfo responses[MAX_CURL_HANDLES];

    CURLM *multi_handle;
    int   available;
} MultiRestState;

extern CURLSH *GLOBAL_CURL_SHARED_STATE;
extern CURL   *GLOBAL_CURL_INSTANCE;
extern List   *MULTI_REST_STATES;

void curl_support_init(void);

#endif