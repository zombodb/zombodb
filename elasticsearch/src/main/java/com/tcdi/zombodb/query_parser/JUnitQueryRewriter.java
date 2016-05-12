package com.tcdi.zombodb.query_parser;

import org.elasticsearch.client.Client;

/**
 * A {@link QueryRewriter} that is used during testing.
 */
public class JUnitQueryRewriter extends ZomboDBQueryRewriter {

    public JUnitQueryRewriter(Client client, String indexName, String searchPreference, String input, boolean doFullFieldDataLookup) {
        super(client, indexName, searchPreference, input, doFullFieldDataLookup);
    }

    @Override
    protected void performCustomOptimizations(String searchPreference, boolean doFullFieldDataLookup) {
        // none
    }
}
