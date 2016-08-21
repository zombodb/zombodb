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
package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class PostgresCountAction extends BaseRestHandler {

    @Inject
    public PostgresCountAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_pgcount", this);
        controller.registerHandler(POST, "/{index}/_pgcount", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        long start = System.currentTimeMillis();
        long count = -1;

        try {
            boolean isSelectivityQuery = request.paramAsBoolean("selectivity", false);
            BytesRestResponse response;
            QueryAndIndexPair query;

            query = PostgresTIDResponseAction.buildJsonQueryFromRequestContent(client, request, !isSelectivityQuery, true);
            SearchRequestBuilder builder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
            builder.setIndices(query.getIndexName());
            builder.setSize(0);
            builder.setSearchType(SearchType.COUNT);
            builder.setPreference(request.param("preference"));
            builder.setRequestCache(true);
            builder.setFetchSource(false);
            builder.setTrackScores(false);
            builder.setNoFields();
            builder.setQuery(query.getQueryBuilder());

            SearchResponse searchResponse = client.execute(DynamicSearchActionHelper.getSearchAction(), builder.request()).get();

            if (searchResponse.getTotalShards() != searchResponse.getSuccessfulShards())
                throw new Exception(searchResponse.getTotalShards() - searchResponse.getSuccessfulShards() + " shards failed");

            count = searchResponse.getHits().getTotalHits();

            // and return that number as a string
            response = new BytesRestResponse(RestStatus.OK, String.valueOf(count));
            channel.sendResponse(response);
        } catch (Throwable e) {
            if (logger.isDebugEnabled())
                logger.error("Error estimating records", e);
            throw new RuntimeException(e);
        } finally {
            long end = System.currentTimeMillis();
            logger.info("Estimated " + count + " records in " + ((end - start) / 1000D) + " seconds.");
        }
    }
}
