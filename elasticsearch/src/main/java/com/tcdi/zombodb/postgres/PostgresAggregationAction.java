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
package com.tcdi.zombodb.postgres;

import com.tcdi.zombodb.query_parser.rewriters.QueryRewriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.search.aggregations.AggregationBuilders.missing;


public class PostgresAggregationAction extends BaseRestHandler {

    @Inject
    public PostgresAggregationAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_pgagg", this);
        controller.registerHandler(POST, "/{index}/_pgagg", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        try {
            final long start = System.currentTimeMillis();
            SearchRequestBuilder builder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
            String input = request.content().toUtf8();
            final QueryRewriter rewriter = QueryRewriter.Factory.create(client, request.param("index"), request.param("preference"), input, true, true, true);
            QueryBuilder qb = rewriter.rewriteQuery();
            AbstractAggregationBuilder ab = rewriter.rewriteAggregations();
            SuggestBuilder.SuggestionBuilder tsb = rewriter.rewriteSuggestions();

            builder.setIndices(rewriter.getAggregateIndexName());
            builder.setTypes("data");
            builder.setQuery(qb);

            if (ab != null) {
                builder.addAggregation(ab);
                if (!rewriter.hasJsonAggregate() && !rewriter.isAggregateNested()) {
                    builder.addAggregation(missing("missing").field(rewriter.getAggregateFieldName()));
                }
            } else if (tsb != null) {
                builder.addSuggestion(tsb);
            }

            builder.setSize(0);
            builder.setNoFields();
            builder.setSearchType(SearchType.COUNT);
            builder.setPreference(request.param("preference"));
            builder.setRequestCache(true);

            final ActionListener<SearchResponse> delegate = new RestStatusToXContentListener<>(channel);
            client.search(builder.request(), new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    delegate.onResponse(searchResponse);
                    long end = System.currentTimeMillis();
                    logger.info("Aggregated results for " + rewriter.getAggregateIndexName() + " in " + ((end - start) / 1000D) + " seconds.");
                }

                @Override
                public void onFailure(Throwable throwable) {
                    delegate.onFailure(throwable);
                }
            });
        } catch (Exception ee) {
            logger.error("Error performing aggregate", ee);
            throw ee;
        }
    }
}
