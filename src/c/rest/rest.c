/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2018-2019 ZomboDB, LLC
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

#include "rest.h"
#include "zombodb.h"
#include "json/json_support.h"

#include "access/xact.h"

#include <zlib.h>

static size_t curl_write_func(char *ptr, size_t size, size_t nmemb, void *userdata);
static int curl_progress_func(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow);
static bool contains_version_conflict_error(const MultiRestState *state, int i);

extern bool zdb_curl_verbose_guc;

static size_t curl_write_func(char *ptr, size_t size, size_t nmemb, void *userdata) {
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
	StringInfo    response   = (StringInfo) userdata;
	appendBinaryStringInfo(response, ptr, (int) (size * nmemb));
	MemoryContextSwitchTo(oldContext);
	return size * nmemb;
}

/*
 * used to check for Postgres-level interrupts while a curl call is running
 */
/*lint -esym 715,clientp,dltotal,dlnow,ultotal,ulnow */
static int curl_progress_func(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) {
	/* if interrupts are being held, then we don't want to abort this curl request */
	if (InterruptHoldoffCount > 0)
		return 0;

	/*
	 * We only support detecting cancellation if we're actually in a transaction
	 * i.e., we're not trying to COMMIT or ABORT a transaction
	 */
	if (IsTransactionState()) {
		/*
		 * This is what CHECK_FOR_INTERRUPTS() does,
		 * except we want to gracefully exit out of
		 * the libcurl innards before we let Postgres
		 * throw the interrupt.
		 */
		if (QueryCancelPending)
			return -1;
	}
	return 0;
}

static char *do_compression(StringInfo input, int level, uint64 *len) {
	Bytef *compressed = NULL;
	if (input != NULL) {
		int rc;

		*len = compressBound((uLong) input->len);
		compressed = palloc(*len);

		if ((rc = compress2(compressed, len, (Bytef *) input->data, (uLong) input->len, level)) != Z_OK) {
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("compression error, code=%d", rc)));
		}
	}
	return (char *) compressed;
}

MultiRestState *rest_multi_init(int nhandles, bool ignore_version_conflicts) {
	MultiRestState *state = MemoryContextAlloc(TopMemoryContext,  /* because that's where curl is allocated too */
											   sizeof(MultiRestState));
	int            i;

	if (nhandles > MAX_CURL_HANDLES) {
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Number of curl handles (%d) is larger than max (%d)", nhandles, MAX_CURL_HANDLES)));
	}

	state->nhandles     = nhandles;
	state->multi_handle = curl_multi_init();
	state->available    = nhandles;
	for (i = 0; i < nhandles; i++) {
		state->handles[i]    = NULL;
		state->headers[i]    = NULL;
		state->errorbuffs[i] = NULL;
		state->postDatas[i]  = NULL;
		state->responses[i]  = NULL;
		state->vconflicts[i] = ignore_version_conflicts;
	}

	curl_record_multi_handle(state);

	return state;
}

int rest_multi_perform(MultiRestState *state) {
	int still_running;
	CURLMcode mc;

	while ((mc = curl_multi_perform(state->multi_handle, &still_running)) == CURLM_CALL_MULTI_PERFORM)
		CHECK_FOR_INTERRUPTS();

	if (mc != CURLM_OK)
		elog(ERROR, "curl_multi_perform failed.  code=%d", mc);

	return still_running;
}

void rest_multi_call(MultiRestState *state, char *method, StringInfo url, PostDataEntry *postData, int compressionLevel) {
	int i;

	if (state->available == 0) {
		int still_running;

		do {
			CHECK_FOR_INTERRUPTS();

			still_running = rest_multi_perform(state);
		} while (still_running == state->nhandles);

		rest_multi_partial_cleanup(state, false, true);
		if (state->available == 0) {
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
							errmsg("unable to cleanup an available rest_multi slot")));
		}
	}

	for (i = 0; i < state->nhandles; i++) {
		if (state->handles[i] == NULL) {
			CURL       *curl;
			char       *errorbuff;
			StringInfo response;

			curl = state->handles[i] = curl_easy_init();
			if (!state->handles[i]) {
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
								errmsg("unable to initialize curl handle")));
			}

			state->headers[i] = curl_slist_append(state->headers[i], "Content-Type: application/json");
			errorbuff = state->errorbuffs[i] = palloc0(CURL_ERROR_SIZE);
			state->postDatas[i] = postData;
			response = state->responses[i] = makeStringInfo();

			curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0);      /* we want progress ... */
			curl_easy_setopt(curl, CURLOPT_PROGRESSFUNCTION,
							 (curl_progress_callback) curl_progress_func);   /* ... to go here so we can detect a ^C within postgres */
			curl_easy_setopt(curl, CURLOPT_USERAGENT, "zdb");
			curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 0);
			curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
			curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0);
			curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
			curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60 * 60L);  /* timeout of 60 minutes */
			curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
			curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
			curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorbuff);

			curl_easy_setopt(curl, CURLOPT_URL, url->data);
			curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
			curl_easy_setopt(curl, CURLOPT_WRITEDATA, response);
			curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, compressionLevel > 0 ? "" : NULL);
			curl_easy_setopt(curl, CURLOPT_VERBOSE, zdb_curl_verbose_guc);

			if (postData != NULL && compressionLevel > 0) {
				char   *data;
				uint64 len;

				postData->compressed_data = data = do_compression(postData->buff, compressionLevel, &len);

				state->headers[i] = curl_slist_append(state->headers[i], "Content-Encoding: deflate");
				curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, len);
				curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
			} else if (postData != NULL) {
				postData->compressed_data = NULL;
				curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postData->buff->len);
				curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postData->buff->data);
			} else {
				curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0);
				curl_easy_setopt(curl, CURLOPT_POSTFIELDS, NULL);
			}

			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, state->headers[i]);
			curl_easy_setopt(curl, CURLOPT_POST,
							 strcmp(method, "GET") != 0 && postData && postData->buff->data ? 1 : 0);
			curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);

			curl_multi_add_handle(state->multi_handle, curl);
			state->available--;

			rest_multi_perform(state);
			return;
		}
	}
}

void rest_multi_wait_for_all_done(MultiRestState *state) {
    int still_running;
    int repeats = 0;

    do {
        CURLMcode mc;
        int numfds = 0;

        while ((mc = curl_multi_perform(state->multi_handle, &still_running)) == CURLM_CALL_MULTI_PERFORM)
            CHECK_FOR_INTERRUPTS();
        if (mc != CURLM_OK) {
            elog(ERROR, "curl_multi_perform failed.  code=%d", mc);
        }

        /* wait for activity, timeout or "nothing" */
        mc = curl_multi_wait(state->multi_handle, NULL, 0, 10000, &numfds);
        if (mc != CURLM_OK) {
            elog(ERROR, "curl_multi_wait failed.  code=%d", mc);
        }

        /*
         * 'numfds' being zero means either a timeout or no file descriptors to
         * wait for. Try timeout on first occurrence, then assume no file
         * descriptors and no file descriptors to wait for means sleep
         */
        if (!numfds) {
            repeats++;
            if (repeats > 1) {
                if (still_running == 0) {
                    return;
                }
                pg_usleep(100);
            }
        } else {
            repeats = 0;
        }

    } while (still_running);
}

void rest_multi_partial_cleanup(MultiRestState *state, bool finalize, bool fast) {
	CURLMsg *msg;
	int     msgs_left;

	while ((msg = curl_multi_info_read(state->multi_handle, &msgs_left))) {
		if (msg->msg == CURLMSG_DONE) {
			/* this handle is finished, so lets clean it */
			CURL *handle = msg->easy_handle;
			bool found   = false;
			int  i;

			for (i = 0; i < state->nhandles; i++) {
				if (state->handles[i] == handle) {
					CURLcode rc;
					int64    response_code;

					if ((rc           = curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code)) !=
						CURLE_OK) {
						ereport(ERROR,
								(errcode(ERRCODE_IO_ERROR),
										errmsg("problem getting response code: rc=%d", rc)));
					}

					if (msg->data.result != CURLE_OK || response_code != 200 ||
						strstr(state->responses[i]->data, "\"errors\":true")) {
						bool ignoreError = state->vconflicts[i] && contains_version_conflict_error(state, i);

						if (!ignoreError) {
							/* REST endpoint messed up */
							ereport(ERROR,
									(errcode(ERRCODE_IO_ERROR),
											errmsg("i=%d, libcurl error:  handle=%p, %s: %s, response_code=%ld, result=%d",
												   i, handle, state->errorbuffs[i], state->responses[i]->data,
												   response_code, msg->data.result)));
						}
					}

					if (state->errorbuffs[i] != NULL) {
						pfree(state->errorbuffs[i]);
						state->errorbuffs[i] = NULL;
					}
					if (state->postDatas[i] != NULL) {
						PostDataEntry *entry = state->postDatas[i];

						resetStringInfo(entry->buff);
						if (state->postDatas[i]->compressed_data != NULL) {
							pfree(state->postDatas[i]->compressed_data);
							state->postDatas[i]->compressed_data = NULL;
						}
						state->pool[entry->pool_idx] = entry->buff;
						state->postDatas[i]          = NULL;
					}
					if (state->responses[i] != NULL) {
						pfree(state->responses[i]->data);
						pfree(state->responses[i]);
						state->responses[i] = NULL;
					}
					if (state->headers[i] != NULL) {
						curl_slist_free_all(state->headers[i]);
						state->headers[i] = NULL;
					}
					state->handles[i] = NULL;
					state->available++;

					found = true;
					break;
				}
			}

			if (found) {
				curl_multi_remove_handle(state->multi_handle, handle);
				curl_easy_cleanup(handle);
				if (fast)
					return;
			} else {
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
								errmsg("couldn't find easy_handle for %p", handle)));
			}
		}
	}

	if (finalize) {
		curl_multi_cleanup(state->multi_handle);
		curl_forget_multi_handle(state);
	}
}

static bool contains_version_conflict_error(const MultiRestState *state, int i) {
	bool ignoreError = false;
	char *json       = parse_json_object(state->responses[i], CurrentMemoryContext);

	if (json) {
		char *items = get_json_object_array(json, "items", true);

		if (items) {
			int len = get_json_array_length(items);
			int a_itr;

			for (a_itr = 0; a_itr < len; a_itr++) {
				void *elem = get_json_array_element_object(items, a_itr, CurrentMemoryContext);
				if (elem) {
					void *update = get_json_object_object(elem, "update", true);
					if (update) {
						void *error = get_json_object_object(update, "error", true);
						if (error) {
							const char *type = get_json_object_string(error, "type", false);
							if (type) {
								if (strcmp("version_conflict_engine_exception", type) == 0) {
									ignoreError = true;
									break;
								}
							}
						}
					}
				}
			}
		}
		pfree(json);
	}

	return ignoreError;
}

StringInfo rest_call(char *method, StringInfo url, StringInfo postData, int compressionLevel) {
	char              *compressed_data = NULL;
	StringInfo        response         = makeStringInfo();
	CURLcode          ret;
	int64             response_code;
	CURL              *curl            = GLOBAL_CURL_INSTANCE;
	struct curl_slist *headers         = NULL;

	headers = curl_slist_append(headers, "Content-Type: application/json");

	/* these are all the curl options we want set every time we use it */
	curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0);      /* we want progress ... */
	curl_easy_setopt(curl, CURLOPT_PROGRESSFUNCTION,
					 (curl_progress_callback) curl_progress_func);   /* to go here so we can detect a ^C within postgres */
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "zdb");
	curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 0);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
	curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60 * 60L);  /* timeout of 60 minutes */
	curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, GLOBAL_CURL_ERRBUF);

	curl_easy_setopt(curl, CURLOPT_URL, url->data);
	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, response);
	curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, compressionLevel > 0 ? "" : NULL);
	curl_easy_setopt(curl, CURLOPT_VERBOSE, zdb_curl_verbose_guc);

	if (postData != NULL && compressionLevel > 0) {
		uint64 len;

		compressed_data = do_compression(postData, compressionLevel, &len);

		headers = curl_slist_append(headers, "Content-Encoding: deflate");
		curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, len);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, compressed_data);
	} else {
		curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postData ? postData->len : 0);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postData ? postData->data : NULL);
	}

	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	if (postData != NULL || strcmp("POST", method) == 0)
		curl_easy_setopt(curl, CURLOPT_POST, 1);
	else
		curl_easy_setopt(curl, CURLOPT_POST, 0);

	ret = curl_easy_perform(curl);

	/* we might have detected an interrupt in the progress function, so check for sure */
	CHECK_FOR_INTERRUPTS();

	if (ret != CURLE_OK) {
		/* curl messed up */
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
						errmsg("libcurl error-code: %s(%d); message: %s; req=-X%s %s ", curl_easy_strerror(ret), ret,
							   GLOBAL_CURL_ERRBUF, method, url->data)));
	}

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
	if (response_code < 200 || (response_code >= 300 && response_code != 404)) {
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
						errmsg("unexpected http response code from remote server.  code=%ld, response=%s",
							   response_code, response->data)));
	}

	if (compressed_data != NULL)
		pfree(compressed_data);

	if (headers != NULL)
		curl_slist_free_all(headers);

	if (response_code != 404 && strstr(response->data, "{\"error\":") != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
						errmsg("%s", response->data)));

	return response;
}

