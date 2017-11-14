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
package llc.zombodb.query_parser.rewriters;

import llc.zombodb.cross_join.CrossJoinQueryBuilder;
import llc.zombodb.query_parser.ASTExpansion;
import llc.zombodb.query_parser.ASTIndexLink;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * ZomboDB's stock {@link QueryRewriter} that resolves joins during construction
 */
public class ZomboDBQueryRewriter extends QueryRewriter {

    public ZomboDBQueryRewriter(ClusterService clusterService, Client client, String indexName, NamedXContentRegistry contentRegistry, String searchPreference, String input, boolean doFullFieldDataLookup, boolean canDoSingleIndex, boolean needVisibilityOnTopLevel) {
        super(clusterService, client, indexName, contentRegistry, input, searchPreference, doFullFieldDataLookup, canDoSingleIndex, needVisibilityOnTopLevel);
    }

    @Override
    protected void performOptimizations(Client client) {
        super.performOptimizations(client);
    }

    @Override
    protected QueryBuilder build(ASTExpansion node) {
        ASTIndexLink link = node.getIndexLink();
        ASTIndexLink myIndex = metadataManager.getMyIndex();
        QueryBuilder qb;

        if (link == myIndex && !node.isGenerated()) {
            return super.build(node);
        } else {
            if ("(null)".equals(link.getLeftFieldname()))
                return super.build(node);

            if (_isBuildingAggregate)
                return matchAllQuery();

            qb = constantScoreQuery(new CrossJoinQueryBuilder()
                    .clusterName(this.clusterService.getClusterName().value())
                    .host(this.clusterService.localNode().getAddress().getHost())
                    .port(this.clusterService.localNode().getAddress().getPort())
                    .index(link.getIndexName())
                    .type("data")
                    .leftFieldname(link.getLeftFieldname())
                    .rightFieldname(link.getRightFieldname())
                    .query(applyVisibility(build(node.getQuery()))));
        }

        if (node.getFilterQuery() != null) {
            BoolQueryBuilder bqb = boolQuery();

            bqb.must(qb);
            bqb.filter(constantScoreQuery(build(node.getFilterQuery())));

            qb = bqb;
        }

        return qb;
    }
}
