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
package com.tcdi.zombodb.postgres;

import com.tcdi.zombodb.postgres.util.OverloadedContentRestRequest;
import com.tcdi.zombodb.postgres.util.QueryAndIndexPair;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Created by e_ridge on 10/17/14.
 */
public class PostgresCountAction extends BaseRestHandler {

    @Inject
    public PostgresCountAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/{type}/_pgcount", this);
        controller.registerHandler(POST, "/{index}/{type}/_pgcount", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        long start = System.currentTimeMillis();
        long count = -1;

        try {
            boolean isSelectivityQuery = request.paramAsBoolean("selectivity", false);
            BytesRestResponse response;
            SearchRequest searchRequest;
            QueryAndIndexPair query;

            query = PostgresTIDResponseAction.buildJsonQueryFromRequestContent(client, request, false, !isSelectivityQuery);
            request = new OverloadedContentRestRequest(request, new BytesArray(query.getQuery()));
            request.params().put("index", query.getIndexName());
            request.params().put("type", "data");
            request.params().put("size", "0");
            request.params().put("_source", "false");

            // perform the search
            searchRequest = RestSearchAction.parseSearchRequest(request);
            searchRequest.listenerThreaded(false);
            searchRequest.searchType(SearchType.COUNT);
            searchRequest.preference(request.param("preference"));


            SearchResponse searchResponse = client.search(searchRequest).get();

            if (searchResponse.getTotalShards() != searchResponse.getSuccessfulShards())
                throw new Exception(searchResponse.getTotalShards() - searchResponse.getSuccessfulShards() + " shards failed");

            count = searchResponse.getHits().getTotalHits();

            // and return that number as a string
            response = new BytesRestResponse(RestStatus.OK, String.valueOf(count));
            channel.sendResponse(response);
        } catch (Throwable e) {
//            if (logger.isDebugEnabled())
            logger.error("Error estimating records", e);
            throw new RuntimeException(e);
        } finally {
            long end = System.currentTimeMillis();
            logger.info("Estimated " + count + " records in " + ((end-start)/1000D) + " seconds.");
        }
    }
}
