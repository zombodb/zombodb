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
#ifndef __REST_H__
#define __REST_H__

#include <curl/curl.h>
#include "postgres.h"
#include "lib/stringinfo.h"

#define MAX_CURL_HANDLES 12
typedef struct MultiRestState {
    CURL *handles[MAX_CURL_HANDLES];
    char *errorbuffs[MAX_CURL_HANDLES];
    StringInfo postDatas[MAX_CURL_HANDLES];
    StringInfo responses[MAX_CURL_HANDLES];

    CURLM *multi_handle;
    int available;
} MultiRestState;

extern StringInfo rest_call(char *method, char *url, char *params, StringInfo postData);
extern StringInfo rest_call_with_lock(char *method, char *url, char *params, StringInfo postData, int64 mutex, bool shared, bool allowCancel);

extern void rest_multi_init(MultiRestState *state);
extern int rest_multi_call(MultiRestState *state, char *method, char *url, StringInfo postData, bool process);
extern bool rest_multi_is_available(MultiRestState *state);
extern bool rest_multi_all_done(MultiRestState *state);
extern void rest_multi_partial_cleanup(MultiRestState *state, bool finalize, bool fast);
#endif
