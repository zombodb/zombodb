/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2013-2015 ZomboDB, LLC
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

import llc.zombodb.query_parser.rewriters.QueryRewriter;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestionBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.search.aggregations.AggregationBuilders.missing;


public class ZomboDBAggregationAction extends BaseRestHandler {

    private final ClusterService clusterService;

    @Inject
    public ZomboDBAggregationAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);

        this.clusterService = clusterService;

        controller.registerHandler(GET, "/{index}/_pgagg", this);
        controller.registerHandler(POST, "/{index}/_pgagg", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final long start = System.currentTimeMillis();
        SearchRequestBuilder builder = SearchAction.INSTANCE.newRequestBuilder(client);
        String input = request.content().utf8ToString();
        final QueryRewriter rewriter = QueryRewriter.Factory.create(clusterService, request, client, request.param("index"), input, true, true);
        QueryBuilder qb = rewriter.rewriteQuery();
        AggregationBuilder ab = rewriter.rewriteAggregations();
        SuggestionBuilder sb = rewriter.rewriteSuggestions();

        builder.setIndices(rewriter.getAggregateIndexName());
        builder.setTypes("data");
        builder.setQuery(qb);

        if (ab != null) {
            builder.addAggregation(ab);
            if (!rewriter.hasJsonAggregate() && !rewriter.isAggregateNested()) {
                builder.addAggregation(missing("missing").field(rewriter.getAggregateFieldName()));
            }
        } else if (sb != null) {
            builder.suggest(new SuggestBuilder().addSuggestion(sb.field(), sb));
        }

        builder.setSize(0);
        builder.setPreference(request.param("preference"));
        builder.setRequestCache(true);

        SearchResponse response = client.search(builder.request()).actionGet();
        return channel -> client.search(builder.request(), new RestStatusToXContentListener<>(channel));
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}
