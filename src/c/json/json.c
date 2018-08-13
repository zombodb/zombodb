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

#include "json.h"

#include <stdlib.h>

#if defined(__clang__)
#pragma clang diagnostic push

// we do one big allocation via malloc, then cast aligned slices of this for
// our structures - we don't have a way to tell the compiler we know what we
// are doing, so disable the warning instead!
#pragma clang diagnostic ignored "-Wcast-align"
#elif defined(_MSC_VER)
#pragma warning(push)

// disable 'function selected for inline expansion' warning
#pragma warning(disable : 4711)
#endif

struct json_parse_state_s {
	const char              *src;
	size_t                  size;
	size_t                  offset;
	char                    *data;
	char                    *dom;
	size_t                  dom_size;
	size_t                  data_size;
	size_t                  line_no;     // line counter for error reporting
	size_t                  line_offset; // (offset-line_offset) is the character number (in bytes)
	enum json_parse_error_e error;
};

static int json_is_hexadecimal_digit(const char c) {
	return (('0' <= c && c <= '9') || ('a' <= c && c <= 'f') ||
			('A' <= c && c <= 'F'));
}

static inline int json_skip_whitespace(struct json_parse_state_s *state) {
	size_t       offset = state->offset;
	const size_t size   = state->size;

	// the only valid whitespace according to ECMA-404 is ' ', '\n', '\r' and '\t'
	switch (state->src[offset]) {
		default:
			return 0;
		case ' ':
		case '\r':
		case '\t':
		case '\n':
			break;
	}

	for (; offset < size; offset++) {
		switch (state->src[offset]) {
			default:
				// Update offset
				state->offset = offset;
				return 1;
			case ' ':
			case '\r':
			case '\t':
				break;
			case '\n':
				state->line_no++;
				state->line_offset = offset;
				break;
		}
	}

	// Update offset
	state->offset = offset;
	return 1;
}

static int json_skip_all_skippables(struct json_parse_state_s *state) {
	// skip all whitespace and other skippables until there are none left
	// note that the previous version suffered from read past errors should
	// the stream end on json_skip_c_style_comments eg. '{"a" ' with comments flag

	int          did_consume = 0;
	const size_t size        = state->size;

	do {
		if (state->offset == size) {
			state->error = json_parse_error_premature_end_of_buffer;
			return 1;
		}

		did_consume = json_skip_whitespace(state);
	} while (0 != did_consume);

	if (state->offset == size) {
		state->error = json_parse_error_premature_end_of_buffer;
		return 1;
	}

	return 0;
}

static int json_get_value_size(struct json_parse_state_s *state);

static int json_get_string_size(struct json_parse_state_s *state) {
	size_t       offset          = state->offset;
	const size_t size            = state->size;
	size_t       data_size       = 0;
	const int    is_single_quote = '\'' == state->src[offset];
	const char   quote_to_use    = is_single_quote ? '\'' : '"';

	state->dom_size += sizeof(struct json_string_s);

	if ('"' != state->src[offset]) {
		// if we are allowed single quoted strings check for that too
		state->error  = json_parse_error_expected_opening_quote;
		state->offset = offset;
		return 1;
	}

	// skip leading '"' or '\''
	offset++;

	while ((offset < size) && (quote_to_use != state->src[offset])) {
		// add space for the character
		data_size++;

		if ('\\' == state->src[offset]) {
			// skip reverse solidus character
			offset++;

			if (offset == size) {
				state->error  = json_parse_error_premature_end_of_buffer;
				state->offset = offset;
				return 1;
			}

			switch (state->src[offset]) {
				default:
					state->error  = json_parse_error_invalid_string_escape_sequence;
					state->offset = offset;
					return 1;
				case '"':
				case '\\':
				case '/':
				case 'b':
				case 'f':
				case 'n':
				case 'r':
				case 't':
					// all valid characters!
					offset++;
					break;
				case 'u':
					if (offset + 5 < size) {
						// invalid escaped unicode sequence!
						state->error  = json_parse_error_invalid_string_escape_sequence;
						state->offset = offset;
						return 1;
					} else if (!json_is_hexadecimal_digit(state->src[offset + 1]) ||
							   !json_is_hexadecimal_digit(state->src[offset + 2]) ||
							   !json_is_hexadecimal_digit(state->src[offset + 3]) ||
							   !json_is_hexadecimal_digit(state->src[offset + 4])) {
						// escaped unicode sequences must contain 4 hexadecimal digits!
						state->error  = json_parse_error_invalid_string_escape_sequence;
						state->offset = offset;
						return 1;
					}

					// valid sequence!
					state->offset += 5;

					// add space for the 5 character sequence too
					data_size += 5;
					break;
			}
		} else if (('\r' == state->src[offset]) || ('\n' == state->src[offset])) {
			// invalid escaped unicode sequence!
			state->error  = json_parse_error_invalid_string_escape_sequence;
			state->offset = offset;
			return 1;
		} else {
			// skip character (valid part of sequence)
			offset++;
		}
	}

	// skip trailing '"' or '\''
	offset++;

	// add enough space to store the string
	state->data_size += data_size;

	// one more byte for null terminator ending the string!
	state->data_size++;

	// update offset
	state->offset = offset;

	return 0;
}

static int json_get_object_size(struct json_parse_state_s *state) {
	size_t elements    = 0;
	int    allow_comma = 0;

	if ('{' != state->src[state->offset]) {
		state->error = json_parse_error_unknown;
		return 1;
	}

	// skip leading '{'
	state->offset++;

	state->dom_size += sizeof(struct json_object_s);

	while (state->offset < state->size) {
		if (json_skip_all_skippables(state)) {
			state->error = json_parse_error_premature_end_of_buffer;
			return 1;
		}
		if ('}' == state->src[state->offset]) {
			// skip trailing '}'
			state->offset++;

			// finished the object!
			break;
		}

		// if we parsed at least once element previously, grok for a comma
		if (allow_comma) {
			if (',' == state->src[state->offset]) {
				// skip comma
				state->offset++;
				allow_comma = 0;
			} else {
				// otherwise we are required to have a comma, and we found none
				state->error = json_parse_error_expected_comma_or_closing_bracket;
				return 1;
			}

			if (json_skip_all_skippables(state)) {
				state->error = json_parse_error_premature_end_of_buffer;
				return 1;
			}
		}

		if (json_get_string_size(state)) {
			// key parsing failed!
			state->error = json_parse_error_invalid_string;
			return 1;
		}

		if (json_skip_all_skippables(state)) {
			state->error = json_parse_error_premature_end_of_buffer;
			return 1;
		}

		if (':' != state->src[state->offset]) {
			state->error = json_parse_error_expected_colon;
			return 1;
		}

		// skip colon
		state->offset++;

		if (json_skip_all_skippables(state)) {
			state->error = json_parse_error_premature_end_of_buffer;
			return 1;
		}

		if (json_get_value_size(state)) {
			// value parsing failed!
			return 1;
		}

		// successfully parsed a name/value pair!
		elements++;
		allow_comma = 1;
	}

	state->dom_size += sizeof(struct json_object_element_s) * elements;

	return 0;
}

static int json_get_array_size(struct json_parse_state_s *state) {
	size_t elements    = 0;
	int    allow_comma = 0;

	if ('[' != state->src[state->offset]) {
		// expected array to begin with leading '['
		state->error = json_parse_error_unknown;
		return 1;
	}

	// skip leading '['
	state->offset++;

	state->dom_size += sizeof(struct json_array_s);

	while (state->offset < state->size) {
		if (json_skip_all_skippables(state)) {
			state->error = json_parse_error_premature_end_of_buffer;
			return 1;
		}

		if (']' == state->src[state->offset]) {
			// skip trailing ']'
			state->offset++;

			state->dom_size += sizeof(struct json_array_element_s) * elements;

			// finished the object!
			return 0;
		}

		// if we parsed at least once element previously, grok for a comma
		if (allow_comma) {
			if (',' == state->src[state->offset]) {
				// skip comma
				state->offset++;
			} else {
				state->error = json_parse_error_expected_comma_or_closing_bracket;
				return 1;
			}

			if (json_skip_all_skippables(state)) {
				state->error = json_parse_error_premature_end_of_buffer;
				return 1;
			}
		}

		if (json_get_value_size(state)) {
			// value parsing failed!
			return 1;
		}

		// successfully parsed an array element!
		elements++;
		allow_comma = 1;
	}

	// we consumed the entire input before finding the closing ']' of the array!
	state->error = json_parse_error_premature_end_of_buffer;
	return 1;
}

static int json_get_number_size(struct json_parse_state_s *state) {
	size_t       offset     = state->offset;
	const size_t size       = state->size;
	int          found_sign = 0;

	state->dom_size += sizeof(struct json_number_s);

	if ((offset < size) && '-' == state->src[offset]) {
		// skip valid leading '-' or '+'
		offset++;

		found_sign = 1;
	}

	if (found_sign && (offset < size) &&
		!('0' <= state->src[offset] && state->src[offset] <= '9')) {
		// check if we are allowing leading '.'
		// a leading '-' must be immediately followed by any digit!
		state->error  = json_parse_error_invalid_number_format;
		state->offset = offset;
		return 1;
	}

	if ((offset < size) && ('0' == state->src[offset])) {
		// skip valid '0'
		offset++;

		if ((offset < size) &&
			('0' <= state->src[offset] && state->src[offset] <= '9')) {
			// a leading '0' must not be immediately followed by any digit!
			state->error  = json_parse_error_invalid_number_format;
			state->offset = offset;
			return 1;
		}
	}

	// the main digits of our number next
	while ((offset < size) && ('0' <= state->src[offset] && state->src[offset] <= '9')) {
		offset++;
	}

	if ((offset < size) && ('.' == state->src[offset])) {
		offset++;

		if (!('0' <= state->src[offset] && state->src[offset] <= '9')) {
			state->error  = json_parse_error_invalid_number_format;
			state->offset = offset;
			return 1;
		}

		// a decimal point can be followed by more digits of course!
		while ((offset < size) &&
			   ('0' <= state->src[offset] && state->src[offset] <= '9')) {
			offset++;
		}
	}

	if ((offset < size) &&
		('e' == state->src[offset] || 'E' == state->src[offset])) {
		// our number has an exponent!
		// skip 'e' or 'E'
		offset++;

		if ((offset < size) &&
			('-' == state->src[offset] || '+' == state->src[offset])) {
			// skip optional '-' or '+'
			offset++;
		}

		// consume exponent digits
		while ((offset < size) &&
			   ('0' <= state->src[offset] && state->src[offset] <= '9')) {
			offset++;
		}
	}

	if (offset < size) {
		switch (state->src[offset]) {
			case ' ':
			case '\t':
			case '\r':
			case '\n':
			case '}':
			case ',':
			case ']':
				// all of the above are ok
				break;
			case '=':
			default:
				state->error  = json_parse_error_invalid_number_format;
				state->offset = offset;
				return 1;
		}
	}

	state->data_size += offset - state->offset;

	// one more byte for null terminator ending the number string!
	state->data_size++;

	// update offset
	state->offset = offset;

	return 0;
}

static int json_get_value_size(struct json_parse_state_s *state) {

	state->dom_size += sizeof(struct json_value_s);

	if (json_skip_all_skippables(state)) {
		state->error = json_parse_error_premature_end_of_buffer;
		return 1;
	}
	switch (state->src[state->offset]) {
		case '"':
			return json_get_string_size(state);
		case '\'':
			// invalid value!
			state->error = json_parse_error_invalid_value;
			return 1;
		case '{':
			return json_get_object_size(state);
		case '[':
			return json_get_array_size(state);
		case '-':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			return json_get_number_size(state);
		case '+':
			// invalid value!
			state->error = json_parse_error_invalid_number_format;
			return 1;
		case '.':
			// invalid value!
			state->error = json_parse_error_invalid_number_format;
			return 1;
		default:
			if ((state->offset + 4) <= state->size &&
				't' == state->src[state->offset + 0] &&
				'r' == state->src[state->offset + 1] &&
				'u' == state->src[state->offset + 2] &&
				'e' == state->src[state->offset + 3]) {
				state->offset += 4;
				return 0;
			} else if ((state->offset + 5) <= state->size &&
					   'f' == state->src[state->offset + 0] &&
					   'a' == state->src[state->offset + 1] &&
					   'l' == state->src[state->offset + 2] &&
					   's' == state->src[state->offset + 3] &&
					   'e' == state->src[state->offset + 4]) {
				state->offset += 5;
				return 0;
			} else if ((state->offset + 4) <= state->size &&
					   'n' == state->src[state->offset + 0] &&
					   'u' == state->src[state->offset + 1] &&
					   'l' == state->src[state->offset + 2] &&
					   'l' == state->src[state->offset + 3]) {
				state->offset += 4;
				return 0;
			}

			// invalid value!
			state->error = json_parse_error_invalid_value;
			return 1;
	}
}

static void json_parse_value(struct json_parse_state_s *state, struct json_value_s *value);

static void json_parse_string(struct json_parse_state_s *state,
							  struct json_string_s *string) {
	size_t       offset        = state->offset;
	const size_t size          = state->size;
	size_t       bytes_written = 0;
	const char   quote_to_use  = '\'' == state->src[offset] ? '\'' : '"';

	string->string = state->data;

	// skip leading '"' or '\''
	offset++;

	do {
		if ('\\' == state->src[offset]) {
			// skip the reverse solidus
			offset++;

			switch (state->src[offset++]) {
				default:
					return; // we cannot ever reach here
				case '"':
					state->data[bytes_written++] = '"';
					break;
				case '\\':
					state->data[bytes_written++] = '\\';
					break;
				case '/':
					state->data[bytes_written++] = '/';
					break;
				case 'b':
					state->data[bytes_written++] = '\b';
					break;
				case 'f':
					state->data[bytes_written++] = '\f';
					break;
				case 'n':
					state->data[bytes_written++] = '\n';
					break;
				case 'r':
					state->data[bytes_written++] = '\r';
					break;
				case 't':
					state->data[bytes_written++] = '\t';
					break;
				case '\r':
					state->data[bytes_written++] = '\r';

					// check if we have a "\r\n" sequence
					if ('\n' == state->src[offset]) {
						state->data[bytes_written++] = '\n';
						offset++;
					}

					break;
				case '\n':
					state->data[bytes_written++] = '\n';
					break;
			}
		} else {
			// copy the character
			state->data[bytes_written++] = state->src[offset++];
		}
	} while (offset < size && (quote_to_use != state->src[offset]));

	// skip trailing '"' or '\''
	offset++;

	// record the size of the string
	string->string_size = bytes_written;

	// add null terminator to string
	state->data[bytes_written++] = '\0';

	// move data along
	state->data += bytes_written;

	// update offset
	state->offset = offset;
}

static void json_parse_key(struct json_parse_state_s *state,
						   struct json_string_s *string) {
	// we are only allowed to have quoted keys, so just parse a string!
	json_parse_string(state, string);
}

static void json_parse_object(struct json_parse_state_s *state,
							  int is_global_object,
							  struct json_object_s *object) {
	size_t                       elements    = 0;
	int                          allow_comma = 0;
	struct json_object_element_s *previous   = 0;

	if (is_global_object) {
		// if we skipped some whitespace, and then found an opening '{' of an
		// object, we actually have a normal JSON object at the root of the DOM...
		if ('{' == state->src[state->offset]) {
			// .. and we don't actually have a global object after all!
			is_global_object = 0;
		}
	}

	if (!is_global_object) {
		// skip leading '{'
		state->offset++;
	}

	(void) json_skip_all_skippables(state);

	while (state->offset < state->size) {
		struct json_object_element_s *element = 0;
		struct json_string_s         *string  = 0;
		struct json_value_s          *value   = 0;

		if (!is_global_object) {
			(void) json_skip_all_skippables(state);

			if ('}' == state->src[state->offset]) {
				// skip trailing '}'
				state->offset++;

				// finished the object!
				break;
			}
		} else {
			if (json_skip_all_skippables(state)) {
				// global object ends when the file ends!
				break;
			}
		}

		// if we parsed at least one element previously, grok for a comma
		if (allow_comma) {
			if (',' == state->src[state->offset]) {
				// skip comma
				state->offset++;
				allow_comma = 0;
				continue;
			}
		}

		element = (struct json_object_element_s *) state->dom;

		state->dom += sizeof(struct json_object_element_s);

		if (0 == previous) {
			// this is our first element, so record it in our object
			object->start = element;
		} else {
			previous->next = element;
		}

		previous = element;

		string = (struct json_string_s *) state->dom;
		state->dom += sizeof(struct json_string_s);

		element->name = string;

		json_parse_key(state, string);

		(void) json_skip_all_skippables(state);

		// skip colon or equals
		state->offset++;

		(void) json_skip_all_skippables(state);

		value = (struct json_value_s *) state->dom;
		state->dom += sizeof(struct json_value_s);

		element->value = value;

		json_parse_value(state, value);

		// successfully parsed a name/value pair!
		elements++;
		allow_comma = 1;
	}

	// if we had at least one element, end the linked list
	if (previous) {
		previous->next = 0;
	}

	if (0 == elements) {
		object->start = 0;
	}

	object->length = elements;
}

static void json_parse_array(struct json_parse_state_s *state,
							 struct json_array_s *array) {
	size_t                      elements    = 0;
	int                         allow_comma = 0;
	struct json_array_element_s *previous   = 0;

	// skip leading '['
	state->offset++;

	(void) json_skip_all_skippables(state);

	while (state->offset < state->size) {
		struct json_array_element_s *element = 0;
		struct json_value_s         *value   = 0;

		(void) json_skip_all_skippables(state);

		if (']' == state->src[state->offset]) {
			// skip trailing ']'
			state->offset++;

			// finished the array!
			break;
		}

		// if we parsed at least one element previously, grok for a comma
		if (allow_comma) {
			if (',' == state->src[state->offset]) {
				// skip comma
				state->offset++;
				allow_comma = 0;
				continue;
			}
		}

		element = (struct json_array_element_s *) state->dom;

		state->dom += sizeof(struct json_array_element_s);

		if (0 == previous) {
			// this is our first element, so record it in our array
			array->start = element;
		} else {
			previous->next = element;
		}

		previous = element;

		value = (struct json_value_s *) state->dom;
		state->dom += sizeof(struct json_value_s);

		element->value = value;

		json_parse_value(state, value);

		// successfully parsed an array element!
		elements++;
		allow_comma = 1;
	}

	// end the linked list
	if (previous) {
		previous->next = 0;
	}

	if (0 == elements) {
		array->start = 0;
	}

	array->length = elements;
	array->list   = NULL;
}

static void json_parse_number(struct json_parse_state_s *state,
							  struct json_number_s *number) {
	size_t       offset        = state->offset;
	const size_t size          = state->size;
	size_t       bytes_written = 0;

	number->number = state->data;

	while (offset < size) {
		int end = 0;

		switch (state->src[offset]) {
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
			case '.':
			case 'e':
			case 'E':
			case '+':
			case '-':
				state->data[bytes_written++] = state->src[offset++];
				break;
			default:
				end = 1;
				break;
		}

		if (0 != end) {
			break;
		}
	}

	// record the size of the number
	number->number_size = bytes_written;
	// add null terminator to number string
	state->data[bytes_written++] = '\0';
	// move data along
	state->data += bytes_written;
	// update offset
	state->offset                = offset;
}

static void json_parse_value(struct json_parse_state_s *state, struct json_value_s *value) {
	(void) json_skip_all_skippables(state);

	switch (state->src[state->offset]) {
		case '"':
		case '\'':
			value->type    = json_type_string;
			value->payload = state->dom;
			state->dom += sizeof(struct json_string_s);
			json_parse_string(state, (struct json_string_s *) value->payload);
			break;
		case '{':
			value->type    = json_type_object;
			value->payload = state->dom;
			state->dom += sizeof(struct json_object_s);
			json_parse_object(state, /* is_global_object = */ 0,
							  (struct json_object_s *) value->payload);
			break;
		case '[':
			value->type    = json_type_array;
			value->payload = state->dom;
			state->dom += sizeof(struct json_array_s);
			json_parse_array(state, (struct json_array_s *) value->payload);
			break;
		case '-':
		case '+':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
		case '.':
			value->type    = json_type_number;
			value->payload = state->dom;
			state->dom += sizeof(struct json_number_s);
			json_parse_number(state, (struct json_number_s *) value->payload);
			break;
		default:
			if ((state->offset + 4) <= state->size &&
				't' == state->src[state->offset + 0] &&
				'r' == state->src[state->offset + 1] &&
				'u' == state->src[state->offset + 2] &&
				'e' == state->src[state->offset + 3]) {
				value->type    = json_type_true;
				value->payload = 0;
				state->offset += 4;
			} else if ((state->offset + 5) <= state->size &&
					   'f' == state->src[state->offset + 0] &&
					   'a' == state->src[state->offset + 1] &&
					   'l' == state->src[state->offset + 2] &&
					   's' == state->src[state->offset + 3] &&
					   'e' == state->src[state->offset + 4]) {
				value->type    = json_type_false;
				value->payload = 0;
				state->offset += 5;
			} else if ((state->offset + 4) <= state->size &&
					   'n' == state->src[state->offset + 0] &&
					   'u' == state->src[state->offset + 1] &&
					   'l' == state->src[state->offset + 2] &&
					   'l' == state->src[state->offset + 3]) {
				value->type    = json_type_null;
				value->payload = 0;
				state->offset += 4;
			}
			break;
	}
}

struct json_value_s *
json_parse_ex(const void *src, size_t src_size, void *(*alloc_func_ptr)(void *, size_t), void *user_data, struct json_parse_result_s *result) {
	struct json_parse_state_s state;
	void                      *allocation;
	struct json_value_s       *value;
	size_t                    total_size;
	int                       input_error;

	if (result) {
		result->error         = json_parse_error_none;
		result->error_offset  = 0;
		result->error_line_no = 0;
		result->error_row_no  = 0;
	}

	if (0 == src) {
		// invalid src pointer was null!
		return 0;
	}

	state.src         = (const char *) src;
	state.size        = src_size;
	state.offset      = 0;
	state.line_no     = 1;
	state.line_offset = 0;
	state.error       = json_parse_error_none;
	state.dom_size    = 0;
	state.data_size   = 0;

	input_error = json_get_value_size(&state);

	if (0 == input_error) {
		json_skip_all_skippables(&state);

		if (state.offset != state.size) {
			/* our parsing didn't have an error, but there are characters remaining in
			 * the input that weren't part of the JSON! */

			state.error = json_parse_error_unexpected_trailing_characters;
			input_error = 1;
		}
	}

	if (input_error) {
		// parsing value's size failed (most likely an invalid JSON DOM!)
		if (result) {
			result->error         = state.error;
			result->error_offset  = state.offset;
			result->error_line_no = state.line_no;
			result->error_row_no  = state.offset - state.line_offset;
		}
		return 0;
	}

	// our total allocation is the combination of the dom and data sizes (we
	// first encode the structure of the JSON, and then the data referenced by
	// the JSON values)
	total_size = state.dom_size + state.data_size;

	if (0 == alloc_func_ptr) {
		allocation = malloc(total_size);
	} else {
		allocation = alloc_func_ptr(user_data, total_size);
	}

	if (0 == allocation) {
		// malloc failed!
		if (result) {
			result->error         = json_parse_error_allocator_failed;
			result->error_offset  = 0;
			result->error_line_no = 0;
			result->error_row_no  = 0;
		}

		return 0;
	}

	// reset offset so we can reuse it
	state.offset = 0;

	// reset the line information so we can reuse it
	state.line_no     = 1;
	state.line_offset = 0;

	state.dom  = (char *) allocation;
	state.data = state.dom + state.dom_size;

	value = (struct json_value_s *) state.dom;
	state.dom += sizeof(struct json_value_s);

	json_parse_value(&state, value);

	return (struct json_value_s *) allocation;
}

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(_MSC_VER)
#pragma warning(pop)
#endif
