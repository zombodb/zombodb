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
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#ifndef WIN32
#  include <unistd.h>
#endif
#include <curl/curl.h>
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "utils/json.h"

#include "rest.h"

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


void rest_multi_init(MultiRestState *state) {
    int i;

    state->multi_handle = curl_multi_init();
    state->available = MAX_CURL_HANDLES;
    for (i=0; i<MAX_CURL_HANDLES; i++) {
        state->handles[i] = NULL;
        state->errorbuffs[i] = NULL;
        state->postDatas[i] = NULL;
        state->responses[i] = NULL;
    }
}


int rest_multi_call(MultiRestState *state, char *method, char *url, StringInfo postData, bool process) {
    int i;

    if (state->available == 0)
        rest_multi_partial_cleanup(state, false, false);

    if (state->available > 0) {
        for (i = 0; i < MAX_CURL_HANDLES; i++) {
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

                curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1L);   /* elasticsearch tends to hang after reusing a connection around 8190 times */
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
	struct timeval timeout;
	CURLMcode      mc;
	CURLM          *multi_handle = state->multi_handle;
	int            still_running;
	fd_set         fdread, fdwrite, fdexcep;
	int            maxfd         = -1;
	long           curl_timeo    = -1;

	curl_multi_perform(multi_handle, &still_running);
	if (still_running < MAX_CURL_HANDLES)
		return true;	/* we have space for more */

	/*
	 * all the slots are full, so determine an amount of time to sleep to
	 * while we wait for one or more requests to finish
	 */

	FD_ZERO(&fdread);
	FD_ZERO(&fdwrite);
	FD_ZERO(&fdexcep);

	mc = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd);
	if(mc != CURLM_OK)
		elog(ERROR, "curl_multi_fdset() failed, code %d", mc);

	mc = curl_multi_timeout(multi_handle, &curl_timeo);
	if (mc != CURLM_OK)
		elog(ERROR, "curl_multi_timeout() failed, code %d", mc);

	if (curl_timeo == -1)
		curl_timeo = 100;

	if (maxfd == -1)
	{
#ifdef _WIN32
		Sleep(100);
#else
		sleep((unsigned int) (curl_timeo / 1000));
#endif
	}
	else
	{
		int rc;
		timeout.tv_sec  = curl_timeo / 1000;
		timeout.tv_usec = (curl_timeo % 1000) * 1000;

		rc = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
		if (rc < 0 && errno == EINTR)
			elog(ERROR, "rest_multi_is_available():  select(%i,,,,%li): %i: %s", maxfd + 1, curl_timeo, errno, strerror(errno));
	}

	return false;
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

            for (i=0; i<MAX_CURL_HANDLES; i++) {
                if (state->handles[i] == handle) {
                    long response_code = 0;

                    curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
                    if (msg->data.result != 0 || response_code != 200 || strstr(state->responses[i]->data, "\"errors\":true")) {
                        /* REST endpoint messed up */
                        elog(IsTransactionState() ? ERROR : WARNING, "%s: %s", state->errorbuffs[i], state->responses[i]->data);
                    }

                    state->handles[i] = NULL;
                    state->available++;

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

        state->available = MAX_CURL_HANDLES;
        state->multi_handle = NULL;
    }
}

StringInfo rest_call(char *method, char *url, char *params, StringInfo postData) {
	return rest_call_with_lock(method, url, params, postData, 0, false, true);
}

StringInfo rest_call_with_lock(char *method, char *url, char *params, StringInfo postData, int64 mutex, bool shared, bool allowCancel) {
    CURL *curl;
    struct curl_slist *headers = NULL;
    char *errorbuff;

    StringInfo response = makeStringInfo();
    CURLcode ret;
    int64 response_code;

    errorbuff = (char *) palloc0(CURL_ERROR_SIZE);
    curl = curl_easy_init();

    if (curl) {
    	headers = curl_slist_append(headers, "Transfer-Encoding:");
	    headers = curl_slist_append(headers, "Expect:");

	    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);   /* allow connections to be reused */
		if (allowCancel)
		{
			curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0);      /* we want progress ... */
			curl_easy_setopt(curl, CURLOPT_PROGRESSFUNCTION, curl_progress_func);   /* to go here so we can detect a ^C within postgres */
		}
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
        curl_easy_setopt(curl, CURLOPT_POST, (strcmp(method, "POST") == 0) || (strcmp(method, "GET") != 0 && postData && postData->data) ? 1 : 0);
    } else {
        elog(IsTransactionState() ? ERROR : WARNING, "Unable to initialize libcurl");
    }

//	if (mutex != 0)
//	{
//		if (shared) DirectFunctionCall1(pg_advisory_lock_shared_int8, Int64GetDatum(mutex));
//		else DirectFunctionCall1(pg_advisory_lock_int8, Int64GetDatum(mutex));
//	}

    ret = curl_easy_perform(curl);

//	if (mutex != 0)
//	{
//		if (shared) DirectFunctionCall1(pg_advisory_unlock_shared_int8, Int64GetDatum(mutex));
//		else DirectFunctionCall1(pg_advisory_unlock_int8, Int64GetDatum(mutex));
//	}

    if (allowCancel && IsTransactionState() && InterruptPending) {
        /* we might have detected one in the progress function, so check for sure */
        CHECK_FOR_INTERRUPTS();
    }

    if (ret != 0) {
        /* curl messed up */
        elog(IsTransactionState() ? ERROR : WARNING, "libcurl error-code: %s(%d); message: %s; req=-X%s %s ", curl_easy_strerror(ret), ret, errorbuff, method, url);
    }

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    if (response_code < 200 || (response_code >=300 && response_code != 404)) {
        text *errorText = DatumGetTextP(DirectFunctionCall2(json_object_field_text, CStringGetTextDatum(response->data), CStringGetTextDatum("error")));

        elog(IsTransactionState() ? ERROR : WARNING, "rc=%ld; %s", response_code, errorText != NULL ? TextDatumGetCString(errorText) : response->data);
    }

    if (headers)
    	curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    pfree(errorbuff);

    return response;
}

