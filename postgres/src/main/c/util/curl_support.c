/*
 * Copyright 2015-2016 ZomboDB, LLC
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
#include <curl/curl.h>

#include "postgres.h"
#include "access/xact.h"

#include "curl_support.h"

CURLSH *GLOBAL_CURL_SHARED_STATE = NULL;
CURL   *GLOBAL_CURL_INSTANCE     = NULL;
List   *MULTI_REST_STATES        = NULL;

static void curl_shared_state_xact_cleanup(XactEvent event, void *arg) {
    switch (event) {
        case XACT_EVENT_PRE_PREPARE:
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_ABORT:
            /* cleanup our global curl instance */
            if (GLOBAL_CURL_INSTANCE) {
                curl_easy_cleanup(GLOBAL_CURL_INSTANCE);
                GLOBAL_CURL_INSTANCE = NULL;
            }

            /* cleanup each multi rest state (and contained curl instances) */
            if (MULTI_REST_STATES != NULL) {
                ListCell *lc;
                int      cnt = 0;

                foreach(lc, MULTI_REST_STATES) {
                    MultiRestState *state = (MultiRestState *) lfirst(lc);
                    CURLM          *multi = state->multi_handle;

                    if (multi) {
                        int i;

                        for (i = 0; i < MAX_CURL_HANDLES; i++) {
                            CURL *curl = state->handles[i];

                            if (curl) {
                                curl_multi_remove_handle(multi, curl);
                                curl_easy_cleanup(curl);
                            }
                        }

                        curl_multi_cleanup(multi);
                        cnt++;
                    }
                }

                MULTI_REST_STATES = NULL;
            }

            break;

        default:
            break;
    }
}

void curl_support_init(void) {
    int rc;

    rc = curl_global_init(CURL_GLOBAL_ALL);
    if (rc != 0)
        elog(ERROR, "Problem initializing libcurl:  rc=%d", rc);

    if ((GLOBAL_CURL_SHARED_STATE = curl_share_init()) == NULL)
        elog(ERROR, "Could not initialize libcurl shared state via curl_share_init()");

    RegisterXactCallback(curl_shared_state_xact_cleanup, NULL);
}

