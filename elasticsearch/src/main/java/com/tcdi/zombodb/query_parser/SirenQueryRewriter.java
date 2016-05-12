package com.tcdi.zombodb.query_parser;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import solutions.siren.join.index.query.FilterJoinBuilder;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * A {@link QueryRewriter} that resolves joins using SIREn:
 *      http://siren.solutions/relational-joins-for-elasticsearch-the-siren-join-plugin/
 */
@SuppressWarnings("unused") /** used via reflection */
public class SirenQueryRewriter extends QueryRewriter {

    @SuppressWarnings("unused") /** used via reflection */
    public SirenQueryRewriter(Client client, String indexName, String searchPreference, String input, boolean doFullFieldDataLookup) {
        super(client, indexName, input, null, true);
    }

    @Override
    protected void performCustomOptimizations() {
        // none
    }

    @Override
    protected QueryBuilder build(ASTExpansion node) {
        ASTIndexLink link = node.getIndexLink();
        ASTIndexLink myIndex = metadataManager.getMyIndex();

        if (link.toString().equals(myIndex.toString()) && !node.isGenerated()) {
            QueryBuilder expansionBuilder =  build(node.getQuery());
            QueryParserNode filterQuery = node.getFilterQuery();
            if (filterQuery != null) {
                BoolQueryBuilder bqb = boolQuery();
                bqb.must(applyExclusion(build(node.getQuery()), link.getIndexName()));
                bqb.must(build(filterQuery));
                expansionBuilder = bqb;
            }
            return expansionBuilder;
        } else {
            FilterJoinBuilder fjb = new FilterJoinBuilder(link.getLeftFieldname()).path(link.getRightFieldname()).indices(link.getIndexName());
            if (node.getFilterQuery() != null) {
                BoolQueryBuilder bqb = boolQuery();
                bqb.must(applyExclusion(build(node.getQuery()), link.getIndexName()));
                bqb.must(build(node.getFilterQuery()));
                fjb.query(bqb);
            } else {
                fjb.query(applyExclusion(build(node.getQuery()), link.getIndexName()));
            }

            // chosen through trial and error to get close enough to 1M records
            // in an index with 1M docs so that Postgres will decode to do a
            // sequential scan
            if (!doFullFieldDataLookup)
                fjb.maxTermsPerShard(1024 * 192);

            return filteredQuery(matchAllQuery(), fjb);
        }
    }
}
