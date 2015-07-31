/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015 ZomboDB, LLC
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
#include <sys/time.h>

#include <curl/curl.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"

#include "am/zdbam.h"
#include "rest/rest.h"
#include "util/curl_support.h"
#include "util/zdbutils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(rest_get);

Datum rest_get(PG_FUNCTION_ARGS);

/*
 * Library initialization functions
 */
void _PG_init(void);
void _PG_fini(void);

void _PG_init(void) {
	curl_support_init();

    zdbam_init();
}

void _PG_fini(void) {
    zdbam_fini();
}

Datum rest_get(PG_FUNCTION_ARGS)
{
	char *url = PG_ARGISNULL(0) ? NULL : GET_STR(PG_GETARG_TEXT_P(0));

	if (url == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(rest_call("GET", url, NULL)->data));
}
