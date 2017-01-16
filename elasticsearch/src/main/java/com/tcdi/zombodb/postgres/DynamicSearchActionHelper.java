/*
 * Copyright 2017 ZomboDB, LLC
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
package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.search.*;

/**
 * Dynamically figure out which "Search Action" INSTANCE we should use, based on if the SIREn plugin is installed or not
 */
public class DynamicSearchActionHelper {

    public static Action<SearchRequest, SearchResponse, SearchRequestBuilder> getSearchAction() {
        try {
            Class clazz = Class.forName("solutions.siren.join.action.coordinate.CoordinateSearchAction");
            return (Action) clazz.getDeclaredField("INSTANCE").get(clazz);
        } catch (Exception e) {
            return SearchAction.INSTANCE;
        }
    }

    public static Action<MultiSearchRequest, MultiSearchResponse, MultiSearchRequestBuilder> getMultiSearchAction() {
        try {
            Class clazz = Class.forName("solutions.siren.join.action.coordinate.CoordinateMultiSearchAction");
            return (Action) clazz.getDeclaredField("INSTANCE").get(clazz);
        } catch (Exception e) {
            return MultiSearchAction.INSTANCE;
        }
    }
}
