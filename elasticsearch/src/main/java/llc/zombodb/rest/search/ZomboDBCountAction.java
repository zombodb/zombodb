/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2018 ZomboDB, LLC
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
package llc.zombodb.rest.search;

import llc.zombodb.rest.QueryAndIndexPair;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Collections;


import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBCountAction extends BaseRestHandler {

    private final ClusterService clusterService;

    @Inject
    public ZomboDBCountAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);

        this.clusterService = clusterService;

        controller.registerHandler(GET, "/{index}/_pgcount", this);
        controller.registerHandler(POST, "/{index}/_pgcount", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        long start = System.currentTimeMillis();
        long count = -1;

        try {
            boolean isSelectivityQuery = request.paramAsBoolean("selectivity", false);
            BytesRestResponse response;
            QueryAndIndexPair query;

            query = ZomboDBTIDResponseAction.buildJsonQueryFromRequestContent(clusterService, client, request, true, false);
            if (query.hasLimit() && isSelectivityQuery) {
                count = query.getLimit().getLimit();
            } else {
                SearchRequestBuilder builder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
                builder.setIndices(query.getIndexName());
                builder.setTypes("data");
                builder.setSize(0);
                builder.setPreference(request.param("preference"));
                builder.setRequestCache(true);
                builder.setFetchSource(false);
                builder.setTrackScores(false);
                builder.setQuery(query.getQueryBuilder());
                builder.setPostFilter(query.getVisibilityFilter());

                SearchResponse searchResponse = client.search(builder.request()).actionGet();

                if (searchResponse.getFailedShards() > 0) {
                    /* didn't work, so return failure */
                    XContentBuilder xContentBuilder = XContentBuilder.builder(JsonXContent.jsonXContent).prettyPrint();
                    searchResponse.toXContent(xContentBuilder, new ToXContent.MapParams(Collections.emptyMap()));
                    return channel -> channel.sendResponse(new BytesRestResponse(searchResponse.status(), xContentBuilder));
                }

                count = searchResponse.getHits().getTotalHits();
            }

            // and return that number as a string
            long finalCount = count;
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.valueOf(finalCount)));
        } finally {
            long end = System.currentTimeMillis();
            logger.info("Estimated " + count + " records in " + ((end - start) / 1000D) + " seconds.");
        }
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}
