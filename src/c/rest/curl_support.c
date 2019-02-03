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

#include "curl_support.h"

#include "access/xact.h"
#include "utils/memutils.h"


static List *curlMultiHandles = NULL;

CURL *GLOBAL_CURL_INSTANCE;
char GLOBAL_CURL_ERRBUF[CURL_ERROR_SIZE];

/*
 * Cleanup libcurl allocated objects when the transaction finishes
 */
static void curl_cleanup_callback(XactEvent event, void *arg) {

	/* handle additional cleanup when the xact aborts */
	switch (event) {
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT: {
			ListCell *lc;

			foreach (lc, curlMultiHandles) {
				MultiRestState *state = lfirst(lc);

				if (state != NULL && state->multi_handle != NULL) {
					int i;

					for (i = 0; i < state->nhandles; i++) {
						if (state->handles[i] != NULL) {
							curl_multi_remove_handle(state->multi_handle, state->handles[i]);
							curl_easy_cleanup(state->handles[i]);
						}
						if (state->headers[i] != NULL) {
							curl_slist_free_all(state->headers[i]);
						}
					}
					curl_multi_cleanup(state->multi_handle);
				}
			}
		}
			break;
		default:
			break;
	}

	/* normal cleanup when transaction completes */
	switch (event) {
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			list_free_deep(curlMultiHandles);
			curlMultiHandles = NULL;
			break;

		default:
			break;
	}
}

/*
 * Initialize libcurl support for the current session.
 *
 * Should only be called once -- when ZomboDB is loaded by the active backend
 */
void curl_support_init(void) {
	CURLcode rc;

	/* initialize libcurl */
	rc = curl_global_init(CURL_GLOBAL_ALL);
	if (rc != CURLE_OK) {
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
						errmsg("Problem initializing libcurl:  rc=%d", rc)));
	}

	GLOBAL_CURL_INSTANCE = curl_easy_init();
	if (GLOBAL_CURL_INSTANCE == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
						errmsg("Error initializing our GLOBAL_CURL_INSTANCE")));
	}
	memset(GLOBAL_CURL_ERRBUF, 0, CURL_ERROR_SIZE);

	/* A callback for freeing libcurl-allocated objects when the transaction completes */
	RegisterXactCallback(curl_cleanup_callback, NULL);
}

void curl_record_multi_handle(MultiRestState *state) {
	MemoryContext oldContext;

	oldContext       = MemoryContextSwitchTo(TopMemoryContext);
	curlMultiHandles = lappend(curlMultiHandles, state);
	MemoryContextSwitchTo(oldContext);
}

void curl_forget_multi_handle(MultiRestState *state) {
	curlMultiHandles = list_delete(curlMultiHandles, state);
	pfree(state);
}

