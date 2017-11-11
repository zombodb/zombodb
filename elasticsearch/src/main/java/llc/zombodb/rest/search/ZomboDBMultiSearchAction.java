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
package llc.zombodb.rest.search;

import llc.zombodb.query_parser.rewriters.QueryRewriter;
import llc.zombodb.query_parser.utils.Utils;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ZomboDBMultiSearchAction extends BaseRestHandler {

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

    private final ClusterService clusterService;

    @Inject
    public ZomboDBMultiSearchAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;

        controller.registerHandler(GET, "/{index}/_zdbmsearch", this);
        controller.registerHandler(POST, "/{index}/_zdbmsearch", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ZDBMultiSearchDescriptor[] descriptors = Utils.jsonToObject(request.content().streamInput(), ZDBMultiSearchDescriptor[].class);
        MultiSearchRequestBuilder msearchBuilder = new MultiSearchRequestBuilder(client, MultiSearchAction.INSTANCE);
        String index = request.param("index");  // appease Elasticsearch request parameter usage checking

        for (ZDBMultiSearchDescriptor md : descriptors) {
            SearchRequestBuilder srb = new SearchRequestBuilder(client, SearchAction.INSTANCE);

            srb.setIndices(md.getIndexName());
            srb.setTypes("data");
            if (md.getPkey() != null) srb.addFieldDataField(md.getPkey());
            srb.setQuery(QueryRewriter.Factory.create(clusterService, request, client, md.getIndexName(), md.getPreference(), md.getQuery(), true, false, true).rewriteQuery());

            msearchBuilder.add(srb);
        }
        return channel -> client.multiSearch(msearchBuilder.request(), new RestToXContentListener<>(channel));
    }

    @Override
    public boolean supportsPlainText() {
        return true;
    }
}
