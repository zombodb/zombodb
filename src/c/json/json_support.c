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

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/fmgrprotos.h"

#include "json_support.h"
#include "json.h"

#define JSON_ERROR(e) \
    ereport(ERROR, \
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), \
                    errmsg("Error parsing json: code=%ld", (e).error)));


void json_support_init(void) {
	// noop
}

/*lint -esym 715,user_data */
static void *json_alloc(void *user_data, size_t size) {
	return palloc(size);
}

bool is_json(char *input) {
	struct json_value_s *jv;
	bool                rc;

	jv = json_parse_ex(input, strlen(input), 0, json_alloc, NULL, NULL);
	rc = jv != NULL && jv->type == json_type_object;

	if (jv != NULL)
		pfree(jv);
	return rc;
}

void *parse_json_object(StringInfo jsonString, MemoryContext memcxt) {
	MemoryContext              oldContext = MemoryContextSwitchTo(memcxt);
	struct json_value_s        *jv;
	struct json_parse_result_s result;

	jv = json_parse_ex(jsonString->data, (size_t) jsonString->len, 0, json_alloc, NULL, &result);
	if (jv == NULL)
		JSON_ERROR(result);

	MemoryContextSwitchTo(oldContext);

	return jv;
}

void *parse_json_object_from_string(char *jsonString, MemoryContext memcxt) {
	MemoryContext              oldContext = MemoryContextSwitchTo(memcxt);
	struct json_value_s        *jv;
	struct json_parse_result_s result;

	jv = json_parse_ex(jsonString, strlen(jsonString), 0, json_alloc, NULL, &result);
	if (jv == NULL)
		JSON_ERROR(result);

	MemoryContextSwitchTo(oldContext);

	return jv;
}

JsonObjectKeyIterator get_json_object_key_iterator(void *object) {
	return ((struct json_object_s *) ((struct json_value_s *) object)->payload)->start;
}

const char *get_key_from_json_object_iterator(JsonObjectKeyIterator itr) {
	return ((struct json_object_element_s *) itr)->name->string;
}

void *get_value_from_json_object_iterator(JsonObjectKeyIterator itr) {
	return ((struct json_object_element_s *) itr)->value->payload;
}

JsonObjectKeyIterator get_next_from_json_object_iterator(JsonObjectKeyIterator itr) {
	return ((struct json_object_element_s *) itr)->next;
}


void *get_json_object_object(void *object, char *key, bool missingOk) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0)
			return elem->value;
	}

	if (missingOk)
		return NULL;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("json value for object key '%s' is not an object", key)));
}

const char *get_json_object_string(void *object, char *key, bool missingOk) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0) {
			struct json_string_s *str = (struct json_string_s *) elem->value->payload;
			return str->string;
		}
	}

	if (missingOk)
		return NULL;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("no such key '%s' in json object", key)));
}

const char *get_json_object_string_force(void *object, char *key) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0) {
			switch (elem->value->type) {
				case json_type_string:
					return ((struct json_string_s *) elem->value->payload)->string;

				case json_type_number:
					return ((struct json_number_s *) elem->value->payload)->number;

				case json_type_false:
					return "false";

				case json_type_true:
					return "true";

				case json_type_null:
					return "null";

				default:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
									errmsg("cannot force value for key '%s' to string", key)));
			}
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("no such key '%s' in json object", key)));
}

uint64 get_json_object_uint64(void *object, char *key, bool missingOk) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0) {
			struct json_number_s *number = (struct json_number_s *) elem->value->payload;

			return DatumGetUInt64(DirectFunctionCall1(int8in, CStringGetDatum(number->number)));
		}
	}

	if (missingOk)
		return 0;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("no such key '%s' in json object", key)));
}

bool get_json_object_bool(void *object, char *key, bool missingOk) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0) {
			struct json_value_s *value = elem->value;
			return value->type == json_type_true;
		}
	}

	if (missingOk)
		return false;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("no such key '%s' in json object", key)));
}

double get_json_object_real(void *object, char *key) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0) {
			struct json_number_s *number = (struct json_number_s *) elem->value->payload;
			if (number == NULL)
				return 0.0;

			return DatumGetFloat8(DirectFunctionCall1(float8in, CStringGetDatum(number->number)));
		}
	}

	return 0.0;
}

uint64 get_json_first_array_uint64(void *object, char *key) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0) {
			struct json_array_s *array = elem->value->payload;
			return DatumGetUInt64(DirectFunctionCall1(int8in, CStringGetDatum(
					((struct json_number_s *) array->start->value->payload)->number)));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("no such key '%s' in json object", key)));
}

void *get_json_object_array(void *object, char *key, bool missing_ok) {
	struct json_value_s          *json = object;
	struct json_object_s         *obj  = (struct json_object_s *) json->payload;
	struct json_object_element_s *elem;

	for (elem = obj->start; elem != NULL; elem = elem->next) {
		if (strcmp(key, elem->name->string) == 0) {
			return elem->value->payload;
		}
	}

	if (missing_ok)
		return NULL;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					errmsg("no such key '%s' in json object", key)));
}

int get_json_array_length(void *array) {
	struct json_array_s *json = array;
	return (int) json->length;
}

static struct json_array_element_s **build_json_list(struct json_array_s *json, MemoryContext memcxt) {
	struct json_array_element_s **list = json->list;

	if (list == NULL) {
		struct json_array_element_s *elem;
		int                         i;

		list = MemoryContextAlloc(memcxt, sizeof(struct json_array_element_s *) * json->length);

		for (elem = json->start, i = 0; elem != NULL; elem = elem->next, i++) {
			list[i] = elem;
		}

		json->list = list;
	}
	return list;
}

void *get_json_array_element_object(void *array, int idx, MemoryContext memcxt) {
	struct json_array_s         *json  = array;
	struct json_array_element_s **list = build_json_list(json, memcxt);

	return list[idx]->value;
}

uint64 get_json_array_element_uint64(void *array, int idx, MemoryContext memcxt) {
	struct json_array_s         *json  = array;
	struct json_array_element_s **list = build_json_list(json, memcxt);

	return DatumGetUInt64(DirectFunctionCall1(int8in, CStringGetDatum(
			((struct json_number_s *) list[idx]->value->payload)->number)));
}

const char *get_json_array_element_string(void *array, int idx, MemoryContext memcxt) {
	struct json_array_s         *json  = array;
	struct json_array_element_s **list = build_json_list(json, memcxt);

	return ((struct json_string_s *) list[idx]->value->payload)->string;
}

char *write_json(void *object) {
	struct json_value_s *json = (struct json_value_s *) object;
	size_t size;

	return json_write_minified(json, &size);
}

