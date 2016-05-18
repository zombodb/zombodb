/*
 * Copyright 2016 ZomboDB, LLC
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcdi.zombodb.query_parser.QueryRewriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZombodbMultiSearchAction extends BaseRestHandler {

    public static class ZDBMultiSearchDescriptor {
        private String indexName;
        private String query;
        private String preference;
        private String pkey;

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getPreference() {
            return preference;
        }

        public void setPreference(String preference) {
            this.preference = preference;
        }

        public String getPkey() {
            return pkey;
        }

        public void setPkey(String pkey) {
            this.pkey = pkey;
        }
    }

    @Inject
    protected ZombodbMultiSearchAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_zdbmsearch", this);
        controller.registerHandler(POST, "/{index}/_zdbmsearch", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, final Client client) throws Exception {
        final long start = System.currentTimeMillis();
        final ZDBMultiSearchDescriptor[] descriptors = new ObjectMapper().readValue(request.content().streamInput(), ZDBMultiSearchDescriptor[].class);
        MultiSearchRequestBuilder msearchBuilder = new MultiSearchRequestBuilder(client);

        for (ZDBMultiSearchDescriptor md : descriptors) {
            SearchRequestBuilder srb = new SearchRequestBuilder(client);

            srb.setIndices(md.getIndexName());
            if (md.getPkey() != null) srb.addFieldDataField(md.getPkey());
            srb.setQuery(QueryRewriter.Factory.create(client, md.getIndexName(), md.getPreference(), md.getQuery(), true).rewriteQuery());

            msearchBuilder.add(srb);
        }

        final ActionListener<MultiSearchResponse> defaultListener = new RestToXContentListener<>(channel);
        client.execute(DynamicSearchActionHelper.getMultiSearchAction(), msearchBuilder.request(), new ActionListener<MultiSearchResponse>() {
            @Override
            public void onResponse(MultiSearchResponse items) {
                long end = System.currentTimeMillis();
                logger.info("Searched " + descriptors.length + " indexes in " + ((end-start)/1000D) + " seconds");
                defaultListener.onResponse(items);
            }

            @Override
            public void onFailure(Throwable e) {
                defaultListener.onFailure(e);
            }
        });
    }
}
