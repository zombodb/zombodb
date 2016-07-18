/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2016 ZomboDB, LLC
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

#include <stdlib.h>
#include <string.h>

#ifndef WIN32

#  include <unistd.h>

#endif

#include "postgres.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "utils/memutils.h"

#include "rest.h"

#include <zlib.h>

static size_t curl_write_func(char *ptr, size_t size, size_t nmemb, void *userdata);
static int    curl_progress_func(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow);

static size_t curl_write_func(char *ptr, size_t size, size_t nmemb, void *userdata) {
    MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
    StringInfo response = (StringInfo) userdata;
    appendBinaryStringInfo(response, ptr, size * nmemb);
    MemoryContextSwitchTo(oldContext);
    return size * nmemb;
}

/** used to check for Postgres-level interrupts. */
static int curl_progress_func(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) {
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
        if (InterruptPending)
            return -1;
    }
    return 0;
}

void rest_multi_init(MultiRestState *state, int nhandles) {
    int i;

    if (nhandles > MAX_CURL_HANDLES)
        elog(ERROR, "Number of curl handles (%d) is larger than max (%d)", nhandles, MAX_CURL_HANDLES);

    state->nhandles     = nhandles;
    state->multi_handle = curl_multi_init();
    state->available    = nhandles;
    for (i = 0; i < nhandles; i++) {
        state->handles[i]    = NULL;
        state->errorbuffs[i] = NULL;
        state->postDatas[i]  = NULL;
        state->responses[i]  = NULL;
    }

    MULTI_REST_STATES = lappend(MULTI_REST_STATES, state);
}

int rest_multi_perform(MultiRestState *state) {
    int still_running;
    CURLMcode rc;

    do {
        rc = curl_multi_perform(state->multi_handle, &still_running);
    } while (rc == CURLM_CALL_MULTI_PERFORM);

    return still_running;
}

void rest_multi_call(MultiRestState *state, char *method, char *url, PostDataEntry *postData) {
    int i;

    if (state->available == 0) {
        int still_running;

        do {
            CHECK_FOR_INTERRUPTS();

            still_running = rest_multi_perform(state);
        } while (still_running == state->nhandles);

        rest_multi_partial_cleanup(state, false, true);
        if (state->available == 0)
            elog(ERROR, "unable to cleanup an available rest_multi slot");
    }

    for (i = 0; i < state->nhandles; i++) {
        if (state->handles[i] == NULL) {
            CURL       *curl;
            char       *errorbuff;
            StringInfo response;

            curl = state->handles[i] = curl_easy_init();
            if (!state->handles[i])
                elog(ERROR, "Unable to initialize CURL handle");

            errorbuff = state->errorbuffs[i] = palloc0(CURL_ERROR_SIZE);
            state->postDatas[i] = postData;
            response = state->responses[i] = makeStringInfo();

            curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1L);   /* reusing connections doesn't make sense because libcurl objects are freed at xact end */
            curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0);      /* we want progress ... */
            curl_easy_setopt(curl, CURLOPT_PROGRESSFUNCTION, (curl_progress_callback) curl_progress_func);   /* ... to go here so we can detect a ^C within postgres */
            curl_easy_setopt(curl, CURLOPT_USERAGENT, "zombodb");
            curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 0);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
            curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0);
            curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorbuff);
            curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60 * 60L);  /* timeout of 60 minutes */
            curl_easy_setopt(curl, CURLOPT_PATH_AS_IS, 1L);

            curl_easy_setopt(curl, CURLOPT_URL, url);
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, response);

            Bytef *compressed = NULL;
            uLongf len = 0;
            if (postData != NULL) {
                struct curl_slist *list = NULL;
                int               rc;

                len        = compressBound((uLong) postData->buff->len);
                compressed = palloc(len);

                if ((rc = compress2(compressed, &len, (Bytef *) postData->buff->data, (uLong) postData->buff->len, 1)) != Z_OK) {
                    elog(ERROR, "compression error: %d", rc);
                }

                list = curl_slist_append(list, "Content-Encoding: deflate");
                curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
            }

            if (compressed != NULL) {
                curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, len);
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, compressed);
            } else {
                curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postData ? postData->buff->len : 0);
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postData ? postData->buff->data : NULL);
            }
            curl_easy_setopt(curl, CURLOPT_POST, strcmp(method, "GET") != 0 && postData && postData->buff->data ? 1 : 0);
            curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_0);
            curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);

            curl_multi_add_handle(state->multi_handle, curl);
            state->available--;

            rest_multi_perform(state);
            return;
        }
    }
}

bool rest_multi_is_available(MultiRestState *state) {
    CURLM *multi_handle         = state->multi_handle;
    int   still_running, numfds = 0;

    /* Has something finished? */
    still_running = rest_multi_perform(state);
    if (still_running < state->nhandles)
        return true;

    /* Not yet, so wait for some action to be performed by curl */
    curl_multi_wait(multi_handle, NULL, 0, 1000, &numfds);
    if (numfds == 0)    /* no action, so get out now */
        return false;

    /* see if something has finished */
    still_running = rest_multi_perform(state);
    return still_running < state->nhandles;
}

bool rest_multi_all_done(MultiRestState *state) {
    int still_running;
    still_running = rest_multi_perform(state);
    return still_running == 0;
}

void rest_multi_partial_cleanup(MultiRestState *state, bool finalize, bool fast) {
    CURLMsg *msg;
    int     msgs_left;

    while ((msg = curl_multi_info_read(state->multi_handle, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE) {
            /** this handle is finished, so lets clean it */
            CURL *handle = msg->easy_handle;
            bool found   = false;
            int  i;

            for (i = 0; i < state->nhandles; i++) {
                if (state->handles[i] == handle) {
                    CURLcode rc;
                    int64 response_code;

                    if ( (rc = curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code)) != CURLE_OK)
                        elog(ERROR, "Problem getting response code: rc=%d", rc);

                    if (msg->data.result != 0 || response_code != 200 ||
                        strstr(state->responses[i]->data, "\"errors\":true")) {
                        /* REST endpoint messed up */
                        elog(ERROR, "i=%d, libcurl error:  handle=%p, %s: %s, response_code=%ld, result=%d", i, handle, state->errorbuffs[i], state->responses[i]->data, response_code, msg->data.result);
                    }

                    if (state->errorbuffs[i] != NULL) {
                        pfree(state->errorbuffs[i]);
                        state->errorbuffs[i] = NULL;
                    }
                    if (state->postDatas[i] != NULL) {
                        PostDataEntry *entry = state->postDatas[i];
                        resetStringInfo(entry->buff);
                        state->pool[entry->pool_idx] = entry->buff;
                    }
                    if (state->responses[i] != NULL) {
                        pfree(state->responses[i]->data);
                        pfree(state->responses[i]);
                        state->responses[i] = NULL;
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
                elog(ERROR, "Couldn't find easy_handle for %p", handle);
            }
        }
    }

    if (finalize) {
        curl_multi_cleanup(state->multi_handle);
        state->multi_handle = NULL;
        state->available    = state->nhandles;
    }
}

StringInfo rest_call(char *method, char *url, StringInfo postData) {
    char *errorbuff = (char *) palloc0(CURL_ERROR_SIZE);

    StringInfo response = makeStringInfo();
    CURLcode   ret;
    int64      response_code;

    GLOBAL_CURL_INSTANCE = curl_easy_init();

    if (GLOBAL_CURL_INSTANCE) {
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_SHARE, GLOBAL_CURL_SHARED_STATE);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_FORBID_REUSE, 1L);   /* reusing connections doesn't make sense because libcurl objects are freed at xact end */
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_NOPROGRESS, 0);      /* we want progress ... */
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_PROGRESSFUNCTION, (curl_progress_callback) curl_progress_func);   /* to go here so we can detect a ^C within postgres */
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_USERAGENT, "zombodb for PostgreSQL");
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_MAXREDIRS, 0);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_WRITEFUNCTION, curl_write_func);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_FAILONERROR, 0);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_ERRORBUFFER, errorbuff);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_NOSIGNAL, 1);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_TIMEOUT, 60 * 60L);  /* timeout of 60 minutes */
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_PATH_AS_IS, 1L);

        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_URL, url);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_CUSTOMREQUEST, method);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_WRITEDATA, response);

        Bytef *compressed = NULL;
        uLongf len = 0;
        if (postData != NULL) {
            struct curl_slist *list = NULL;
            int rc;

            len        = compressBound((uLong) postData->len);
            compressed = palloc(len);

            if ((rc = compress2(compressed, &len, (Bytef *) postData->data, (uLong) postData->len, 1)) != Z_OK) {
                elog(ERROR, "compression error: %d", rc);
            }

            list = curl_slist_append(list, "Content-Encoding: deflate");
            curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_HTTPHEADER, list);
        }

        if (compressed != NULL) {
            curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POSTFIELDSIZE, len);
            curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POSTFIELDS, compressed);
        } else {
            curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POSTFIELDSIZE, postData ? postData->len : 0);
            curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POSTFIELDS, postData ? postData->data : NULL);
        }
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_POST, (strcmp(method, "POST") == 0) || (postData && postData->data) ? 1 : 0);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_0);
        curl_easy_setopt(GLOBAL_CURL_INSTANCE, CURLOPT_TCP_NODELAY, 1L);
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
    if (response_code < 200 || (response_code >= 300 && response_code != 404)) {
        elog(ERROR, "rc=%ld; %s", response_code, response->data);
    }

    pfree(errorbuff);

    curl_easy_cleanup(GLOBAL_CURL_INSTANCE);
    GLOBAL_CURL_INSTANCE = NULL;

    return response;
}

