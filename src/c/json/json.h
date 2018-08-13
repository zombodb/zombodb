// The latest version of this library is available on GitHub;
//   https://github.com/sheredom/json.h

// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <http://unlicense.org/>

#ifndef SHEREDOM_JSON_H_INCLUDED
#define SHEREDOM_JSON_H_INCLUDED

#if defined(_MSC_VER)
#pragma warning(push)

// disable 'bytes padding added after construct' warning
#pragma warning(disable : 4820)
#endif

/*lint -e451 */
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct json_value_s;
struct json_parse_result_s;

// Parse a JSON text file, returning a pointer to the root of the JSON
// structure. json_parse performs 1 call to malloc for the entire encoding.
// Returns 0 if an error occurred (malformed JSON input, or malloc failed). If
// an error occurred, the result struct (if not NULL) will explain the type of
// error, and the location in the input it occurred.
struct json_value_s *json_parse_ex(const void *src, size_t src_size, void *(*alloc_func_ptr)(void *, size_t), void *user_data, struct json_parse_result_s *result);

// The various types JSON values can be. Used to identify what a value is
enum json_type_e {
	json_type_string,
	json_type_number,
	json_type_object,
	json_type_array,
	json_type_true,
	json_type_false,
	json_type_null
};

// A JSON string value
struct json_string_s {
	// utf-8 string
	const char *string;
	// the size (in bytes) of the string
	size_t     string_size;
};

// a JSON number value
struct json_number_s {
	// ASCII string containing representation of the number
	const char *number;
	// the size (in bytes) of the number
	size_t     number_size;
};

// an element of a JSON object
struct json_object_element_s {
	// the name of this element
	struct json_string_s         *name;
	// the value of this element
	struct json_value_s          *value;
	// the next object element (can be NULL if the last element in the object)
	struct json_object_element_s *next;
};

// a JSON object value
struct json_object_s {
	// a linked list of the elements in the object
	struct json_object_element_s *start;
	// the number of elements in the object
	size_t                       length;
};

// an element of a JSON array
struct json_array_element_s {
	// the value of this element
	struct json_value_s         *value;
	// the next array element (can be NULL if the last element in the array)
	struct json_array_element_s *next;
};

// a JSON array value
struct json_array_s {
	// a linked list of the elements in the array
	struct json_array_element_s *start;
	// the number of elements in the array
	size_t                      length;
	void                        *list;
};

// a JSON value
struct json_value_s {
	// a pointer to either a json_string_s, json_number_s, json_object_s, or
	// json_array_s. Should be cast to the appropriate struct type based on what
	// the type of this value is
	void             *payload;
	// must be one of json_type_e. If type is json_type_true, json_type_false, or
	// json_type_null, payload will be NULL
	enum json_type_e type;
};

// a parsing error code
/*lint -e758 it's used, we just ignore json.c */
enum json_parse_error_e {
	// no error occurred (huzzah!)
			json_parse_error_none = 0,

	// expected either a comma or a closing '}' or ']' to close an object or
	// array!
			json_parse_error_expected_comma_or_closing_bracket,

	// colon separating name/value pair was missing!
			json_parse_error_expected_colon,

	// expected string to begin with '"'!
			json_parse_error_expected_opening_quote,

	// invalid escaped sequence in string!
			json_parse_error_invalid_string_escape_sequence,

	// invalid number format!
			json_parse_error_invalid_number_format,

	// invalid value!
			json_parse_error_invalid_value,

	// reached end of buffer before object/array was complete!
			json_parse_error_premature_end_of_buffer,

	// string was malformed!
			json_parse_error_invalid_string,

	// a call to malloc, or a user provider allocator, failed
			json_parse_error_allocator_failed,

	// the JSON input had unexpected trailing characters that weren't part of the
	// JSON value
			json_parse_error_unexpected_trailing_characters,

	// catch-all error for everything else that exploded (real bad chi!)
			json_parse_error_unknown
};

// error report from json_parse_ex()
struct json_parse_result_s {
	// the error code (one of json_parse_error_e)
	enum json_parse_error_e error;

	// the character offset for the error in the JSON input
	size_t error_offset;

	// the line number for the error in the JSON input
	size_t error_line_no;

	// the row number for the error, in bytes
	size_t error_row_no;
};

#ifdef __cplusplus
} // extern "C"
#endif

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

#endif // SHEREDOM_JSON_H_INCLUDED
