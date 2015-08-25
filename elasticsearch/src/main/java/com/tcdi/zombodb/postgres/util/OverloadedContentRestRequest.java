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
package com.tcdi.zombodb.postgres.util;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;

import java.util.Map;

/**
* Created by e_ridge on 10/17/14.
*/
public class OverloadedContentRestRequest extends RestRequest {
    private final RestRequest request;
    private final BytesReference content;

    public OverloadedContentRestRequest(RestRequest request, BytesReference content) {
        this.request = request;
        this.content = content;
    }

    @Override
    public boolean hasContent() {
        return true;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    //
    // all delegated
    //

    @Override
    public Method method() {
        return request.method();
    }

    @Override
    public String uri() {
        return request.uri();
    }

    @Override
    public String rawPath() {
        return request.rawPath();
    }

    @Override
    public String header(String name) {
        return request.header(name);
    }

    @Override
    public Iterable<Map.Entry<String, String>> headers() {
        return request.headers();
    }

    @Override
    public boolean hasParam(String key) {
        return request.hasParam(key);
    }

    @Override
    public String param(String key) {
        return request.param(key);
    }

    @Override
    public Map<String, String> params() {
        return request.params();
    }

    @Override
    public String param(String key, String defaultValue) {
        return request.param(key, defaultValue);
    }
}
