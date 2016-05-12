package com.tcdi.zombodb.query_parser;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

/**
 * ZomboDB's stock {@link QueryRewriter} that resolves joins during construction
 */
public class ZomboDBQueryRewriter extends QueryRewriter {

    public ZomboDBQueryRewriter(Client client, String indexName, String searchPreference, String input, boolean doFullFieldDataLookup) {
        super(client, indexName, input, searchPreference, doFullFieldDataLookup);
    }

    @Override
    protected void performCustomOptimizations(String searchPreference, boolean doFullFieldDataLookup) {
        new ExpansionOptimizer(this, tree, metadataManager, client, searchPreference, doFullFieldDataLookup).optimize();
    }

    @Override
    protected QueryBuilder build(ASTExpansion node) {
        QueryBuilder expansionBuilder =  build(node.getQuery());
        QueryParserNode filterQuery = node.getFilterQuery();
        if (filterQuery != null) {
            BoolQueryBuilder bqb = boolQuery();
            bqb.must(applyExclusion(build(node.getQuery()), node.getIndexLink().getIndexName()));
            bqb.must(build(filterQuery));
            expansionBuilder = bqb;
        }
        return expansionBuilder;
    }
}
