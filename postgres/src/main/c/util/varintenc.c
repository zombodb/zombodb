/**
 * Copyright 2017 ZomboDB, LLC
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
#include "varintenc.h"

char *varint_encode_uint32(uint32 i, int *size) {
	char *rc = palloc(sizeof(uint32));
	int offset = 0;
	while ((i & ~0x7F) != 0) {
		rc[offset++] = ((int8)((i & 0x7F) | 0x80));
		i >>= (unsigned int) 7;
	}
	rc[offset++] = (int8) i;
	*size = offset;
	return rc;
}

char *varint_encode_uint64(uint64 i, int *size) {
	char *rc = palloc(sizeof(uint64));
	int offset = 0;
	while ((i & ~0x7FL) != 0L) {
		rc[offset++] = ((int8)((i & 0x7FL) | 0x80L));
		i >>= (unsigned int) 7;
	}
	rc[offset++] = (int8) i;
	*size = offset;
	return rc;
}