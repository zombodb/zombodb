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
#include <curl/curl.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#ifndef WIN32
#  include <unistd.h>
#endif

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "utils/json.h"

#include "rest.h"
#include "util/curl_support.h"

static size_t curl_write_func(char *ptr, size_t size, size_t nmemb, void *userdata);
static int curl_progress_func(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow);

static size_t curl_write_func(char *ptr, size_t size, size_t nmemb, void *userdata) {
    StringInfo response = (StringInfo) userdata;
    appendBinaryStringInfo(response, ptr, size * nmemb);
    return size * nmemb;
}

/** used to check for Postgres-level interrupts. */
static int curl_progress_func(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) {
	/*
	 * We only support detecting cancellation if we're actually in a transaction
	 * i.e., we're not trying to COMMIT or ABORT a transaction
	 */
	if (IsTransactionState())
	{
		/*
		 * This is what CHECK_FOR_INTERRUPTS() does,
		 * except we want to gracefully exit out of
		 * the libcurl innards before we let Postgres
		 * throw the interrupt.
		 */
		if (InterruptPending)
			return -1;
	}
    return 0;
}

void rest_multi_init(MultiRestState *state, int nhandles) {
    int i;

	if (nhandles > MAX_CURL_HANDLES)
		elog(ERROR, "Number of curl handles (%d) is larger than max (%d)", nhandles, MAX_CURL_HANDLES);

	state->nhandles = nhandles;
    state->multi_handle = curl_multi_init();
    state->available = nhandles;
    for (i=0; i<nhandles; i++) {
        state->handles[i] = NULL;
        state->errorbuffs[i] = NULL;
        state->postDatas[i] = NULL;
        state->responses[i] = NULL;
    }

	MULTI_REST_STATES = lappend(MULTI_REST_STATES, state);
}


int rest_multi_call(MultiRestState *state, char *method, char *url, StringInfo postData, bool process) {
    int i;

    if (state->available == 0)
        rest_multi_partial_cleanup(state, false, false);

    if (state->available > 0) {
        for (i = 0; i < state->nhandles; i++) {
            if (state->handles[i] == NULL) {
                CURL *curl;
                char *errorbuff;
                StringInfo response;
                int still_running;

                curl = state->handles[i] = curl_easy_init();
                if (!state->handles[i])
                    elog(IsTransactionState() ? ERROR : WARNING, "Unable to initialize CURL handle");

                errorbuff = state->errorbuffs[i] = palloc0(CURL_ERROR_SIZE);
                state->postDatas[i] = postData;
                response = state->responses[i] = makeStringInfo();

                curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1L);   /* reusing connections doesn't make sense because libcurl objects are freed at xact end */
                curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0);      /* we want progress ... */
                curl_easy_setopt(curl, CURLOPT_PROGRESSFUNCTION, curl_progress_func);   /* ... to go here so we can detect a ^C within postgres */
                curl_easy_setopt(curl, CURLOPT_USERAGENT, "zombodb for PostgreSQL");
                curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 0);
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0);
                curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorbuff);
                curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
                curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60 * 60L);  /* timeout of 60 minutes */

                curl_easy_setopt(curl, CURLOPT_URL, url);
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, response);
                curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postData ? postData->len : 0);
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postData ? postData->data : NULL);
                curl_easy_setopt(curl, CURLOPT_POST, strcmp(method, "GET") != 0 && postData && postData->data ? 1 : 0);

                curl_multi_add_handle(state->multi_handle, curl);
                state->available--;

                if (process)
                    curl_multi_perform(state->multi_handle, &still_running);

                return i;
            }
        }
    }

    return -1;
}

bool rest_multi_is_available(MultiRestState *state) {
	CURLM *multi_handle = state->multi_handle;
	int still_running, numfds = 0;

	/* Has something finished? */
	curl_multi_perform(multi_handle, &still_running);
	if (still_running < state->nhandles)
		return true;

	/* Not yet, so wait for some action to be performed by curl */
	curl_multi_wait(multi_handle, NULL, 0, 1000, &numfds);
	if (numfds == 0)    /* no action, so get out now */
		return false;

	/* see if something has finished */
	curl_multi_perform(multi_handle, &still_running);
	return still_running < state->nhandles;
}

bool rest_multi_all_done(MultiRestState *state) {
    int still_running;
    curl_multi_perform(state->multi_handle, &still_running);
    return still_running == 0;
}

void rest_multi_partial_cleanup(MultiRestState *state, bool finalize, bool fast) {
    CURLMsg *msg;
    int msgs_left;

    while ((msg = curl_multi_info_read(state->multi_handle, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE) {
            /** this handle is finished, so lets clean it */
            CURL *handle = msg->easy_handle;
            bool found = false;
            int i;

            curl_multi_remove_handle(state->multi_handle, handle);

            for (i=0; i<state->nhandles; i++) {
                if (state->handles[i] == handle) {
                    long response_code = 0;

                    curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
                    if (msg->data.result != 0 || response_code != 200 || strstr(state->responses[i]->data, "\"errors\":true")) {
                        /* REST endpoint messed up */
                        elog(ERROR, "%s: %s", state->errorbuffs[i], state->responses[i]->data);
                    }

                    if (state->errorbuffs[i] != NULL) {
                        pfree(state->errorbuffs[i]);
                        state->errorbuffs[i] = NULL;
                    }
                    if (state->postDatas[i] != NULL) {
                        pfree(state->postDatas[i]->data);
                        pfree(state->postDatas[i]);
                        state->postDatas[i] = NULL;
                    }
                    if (state->responses[i] != NULL) {
                        pfree(state->responses[i]->data);
                        pfree(state->responses[i]);
                        state->responses[i] = NULL;
                    }

					curl_easy_cleanup(handle);
					state->handles[i] = NULL;
					state->available++;

                    if (fast)
                        return;

                    found = true;
                    break;
                }
            }

            if (!found) {
                elog(IsTransactionState() ? ERROR : WARNING, "Couldn't find easy_handle for %p", handle);
            }
        }
    }

    if (finalize) {
        curl_multi_cleanup(state->multi_handle);
        state->multi_handle = NULL;
		state->available = state->nhandles;
    }
}

StringInfo rest_call(char *method, char *url, StringInfo postData)
{
    char *errorbuff = (char *) palloc0(CURL_ERROR_SIZE);

    StringInfo response = makeStringInfo();
    CURLcode ret;
    int64 response_code;

    GLOBAL_CURL_INSTANCE = curl_easy_init();

    if (GLOBAL_CURL_INSTANCE) {
		curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_SHARE, GLOBAL_CURL_SHARED_STATE);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_FORBID_REUSE, 1L);   /* reusing connections doesn't make sense because libcurl objects are freed at xact end */
		curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_NOPROGRESS, 0);      /* we want progress ... */
		curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_PROGRESSFUNCTION, curl_progress_func);   /* to go here so we can detect a ^C within postgres */
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_USERAGENT, "zombodb for PostgreSQL");
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_MAXREDIRS, 0);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_WRITEFUNCTION, curl_write_func);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_FAILONERROR, 0);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_ERRORBUFFER, errorbuff);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_NOSIGNAL, 1);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_TIMEOUT, 60 * 60L);  /* timeout of 60 minutes */

        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_URL, url);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_CUSTOMREQUEST, method);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_WRITEDATA, response);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POSTFIELDSIZE, postData ? postData->len : 0);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POSTFIELDS, postData ? postData->data : NULL);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POST, (strcmp(method, "POST") == 0) || (strcmp(method, "GET") != 0 && postData && postData->data) ? 1 : 0);
    } else {
        elog(ERROR, "Unable to initialize libcurl");
    }

    ret = curl_easy_perform(GLOBAL_CURL_INSTANCE);

	/* we might have detected an interrupt in the progress function, so check for sure */
	CHECK_FOR_INTERRUPTS();

    if (ret != 0) {
        /* curl messed up */
        elog(ERROR, "libcurl error-code: %s(%d); message: %s; req=-X%s %s ", curl_easy_strerror(ret), ret, errorbuff, method, url);
    }

    curl_easy_getinfo(GLOBAL_CURL_INSTANCE, CURLINFO_RESPONSE_CODE, &response_code);
    if (response_code < 200 || (response_code >=300 && response_code != 404)) {
        text *errorText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("error")));

        elog(ERROR, "rc=%ld; %s", response_code, errorText != NULL ? TextDatumGetCString(errorText) : response->data);
    }

    pfree(errorbuff);

	curl_easy_cleanup(GLOBAL_CURL_INSTANCE);
	GLOBAL_CURL_INSTANCE = NULL;

    return response;
}

