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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;

import static org.elasticsearch.action.count.CountRequest.DEFAULT_MIN_SCORE;
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
        } finally {
            long end = System.currentTimeMillis();
            logger.info("Estimated " + count + " records in " + ((end-start)/1000D) + " seconds.");
        }
    }

    public static long countRecords(RestRequest request, Client client) throws InterruptedException, java.util.concurrent.ExecutionException {
        CountRequest countRequest = new CountRequest(Strings.splitStringByCommaToArray(request.param("index")));
        countRequest.indicesOptions(IndicesOptions.fromRequest(request, countRequest.indicesOptions()));
        countRequest.listenerThreaded(false);
        if (request.hasContent()) {
            countRequest.source(request.content());
        } else {
            String source = request.param("source");
            if (source != null) {
                countRequest.source(source);
            } else {
                QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
                if (querySourceBuilder != null) {
                    countRequest.source(querySourceBuilder);
                }
            }
        }
        countRequest.routing(request.param("routing"));
        countRequest.minScore(request.paramAsFloat("min_score", DEFAULT_MIN_SCORE));
        countRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        countRequest.preference(request.param("preference"));

        ActionFuture<CountResponse> countResponse = client.count(countRequest);

        return countResponse.get().getCount();
    }


}
