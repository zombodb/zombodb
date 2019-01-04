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

#ifndef __ZDB_REST_H__
#define __ZDB_REST_H__

#include "curl_support.h"

StringInfo rest_call(char *method, StringInfo url, StringInfo postData, int compressionLevel);

MultiRestState *rest_multi_init(int nhandles, bool ignore_version_conflicts);
int rest_multi_perform(MultiRestState *state);
void rest_multi_call(MultiRestState *state, char *method, StringInfo url, PostDataEntry *postData, int compressionLevel);
void rest_multi_wait_for_all_done(MultiRestState *state);
void rest_multi_partial_cleanup(MultiRestState *state, bool finalize, bool fast);

#endif /* __ZDB_REST_H__ */
