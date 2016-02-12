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

/**
 * Created by e_ridge on 12/17/15.
 */
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
        controller.registerHandler(GET, "/{index}/{type}/_zdbmsearch", this);
        controller.registerHandler(POST, "/{index}/{type}/_zdbmsearch", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, final Client client) throws Exception {
        final long start = System.currentTimeMillis();
        final ZDBMultiSearchDescriptor[] descriptors = new ObjectMapper().readValue(request.content().streamInput(), ZDBMultiSearchDescriptor[].class);
        MultiSearchRequestBuilder msearchBuilder = new MultiSearchRequestBuilder(client);
        String thisType = request.param("type");

        for (ZDBMultiSearchDescriptor md : descriptors) {
            SearchRequestBuilder srb = new SearchRequestBuilder(client);

            srb.setIndices(md.getIndexName());
            srb.setTypes(thisType);
            if (md.getPkey() != null) srb.addFieldDataField(md.getPkey());
            srb.setQuery(new QueryRewriter(client, md.getIndexName(), md.getPreference(), md.getQuery(), true, true, true).rewriteQuery());

            msearchBuilder.add(srb);
        }

        final ActionListener<MultiSearchResponse> defaultListener = new RestToXContentListener<MultiSearchResponse>(channel);
        client.multiSearch(msearchBuilder.request(), new ActionListener<MultiSearchResponse>() {
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
