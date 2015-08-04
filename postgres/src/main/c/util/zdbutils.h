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
#ifndef ZDBUTILS_H
#define ZDBUTILS_H

#include "postgres.h"
#include "lib/stringinfo.h"

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

void appendBinaryStringInfoAndStripLineBreaks(StringInfo str, const char *data, int datalen);
void freeStringInfo(StringInfo si);
char *lookup_primary_key(char *schemaName, char *tableName, bool failOnMissing);

#endif
