/**
 * Copyright 2019 ZomboDB, LLC
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
#include "postgres.h"
#include "lib/stringinfo.h"

#ifndef ZOMBODB_HELPER_FUNCS_H
#define ZOMBODB_HELPER_FUNCS_H

/* === Functions we export to RUST === */
PGDLLEXPORT void zdb_log_proxy(int loglevel, const char *log_msg);
PGDLLEXPORT MemoryContext zdb_GetMemoryChunkContext(void *pointer);

/* === Functions we import from RUST === */
extern StringInfo rest_call(const char *method, StringInfo url, StringInfo postData, int compressionLevel);
extern void rust_init(void);

#endif //ZOMBODB_HELPER_FUNCS_H
