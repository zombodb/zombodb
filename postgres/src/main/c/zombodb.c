/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2019 ZomboDB, LLC
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
#include "utils/builtins.h"

#include "am/zdbam.h"
#include "am/zdb_interface.h"
#include "util/curl_support.h"
#include "rest/rest.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(zdb_es_direct_request);

Datum zdb_es_direct_request(PG_FUNCTION_ARGS);

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

Datum zdb_es_direct_request(PG_FUNCTION_ARGS) {
    Oid                indexrelid     = PG_GETARG_OID(0);
    char               *method        = GET_STR(PG_GETARG_TEXT_P(1));
    char               *endpoint      = GET_STR(PG_GETARG_TEXT_P(2));
    ZDBIndexDescriptor *desc;
    StringInfo         final_endpoint = makeStringInfo();
    StringInfo         response;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);

    if (endpoint[0] == '/') {
        /* user wants to hit the cluster itself */
        appendStringInfo(final_endpoint, "%s%s", desc->url, endpoint+1);
    } else {
        /* user wants to hit the specific index */
        appendStringInfo(final_endpoint, "%s%s/%s", desc->url, desc->fullyQualifiedName, endpoint);
    }
    response = rest_call(method, final_endpoint->data, NULL, 0);
    freeStringInfo(final_endpoint);

    PG_RETURN_TEXT_P(cstring_to_text(response->data));
}
