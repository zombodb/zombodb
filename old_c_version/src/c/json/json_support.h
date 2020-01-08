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

#ifndef __ZDB_JSON_SUPPORT_H__
#define __ZDB_JSON_SUPPORT_H__

#include "postgres.h"
#include "lib/stringinfo.h"

typedef void *JsonObjectKeyIterator;
typedef void *zdb_json_object;

void json_support_init(void);
void *json_alloc(void *user_data, size_t size);

bool is_json(char *input);
void *parse_json_object(StringInfo jsonString, MemoryContext memcxt);
void *parse_json_object_from_string(char *jsonString, MemoryContext memcxt);

JsonObjectKeyIterator get_json_object_key_iterator(void *object);
const char *get_key_from_json_object_iterator(JsonObjectKeyIterator itr);
void *get_value_from_json_object_iterator(JsonObjectKeyIterator itr);
JsonObjectKeyIterator get_next_from_json_object_iterator(JsonObjectKeyIterator itr);

void *get_json_object_object(void *object, char *key, bool missingOk);
uint64 get_json_object_uint64(void *object, char *key, bool missingOk);
bool get_json_object_bool(void *object, char *key, bool missingOk);
double get_json_object_real(void *object, char *key);
const char *get_json_object_string(void *object, char *key, bool missingOk);
const char *get_json_object_string_force(void *object, char *key);
uint64 get_json_first_array_uint64(void *object, char *key);
void *get_json_object_array(void *object, char *key, bool missing_ok);
int get_json_array_length(void *array);
void *get_json_array_element_object(void *array, int idx, MemoryContext memcxt);
uint64 get_json_array_element_uint64(void *array, int idx, MemoryContext memcxt);
const char *get_json_array_element_string(void *array, int idx, MemoryContext memcxt);
char *write_json(void *object);

#endif /* __ZDB_JSON_SUPPORT_H__ */
