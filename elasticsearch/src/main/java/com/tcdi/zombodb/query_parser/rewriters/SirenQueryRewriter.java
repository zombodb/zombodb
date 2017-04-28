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
package com.tcdi.zombodb.query_parser.rewriters;

import com.tcdi.zombodb.query_parser.ASTExpansion;
import com.tcdi.zombodb.query_parser.ASTIndexLink;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import solutions.siren.join.index.query.FilterJoinBuilder;

import static org.elasticsearch.index.query.FilterBuilders.boolFilter;
import static org.elasticsearch.index.query.FilterBuilders.queryFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * A {@link QueryRewriter} that resolves joins using SIREn:
 * http://siren.solutions/relational-joins-for-elasticsearch-the-siren-join-plugin/
 */

@SuppressWarnings("unused") /* used via reflection */
public class SirenQueryRewriter extends QueryRewriter {

    @SuppressWarnings("unused") /* used via reflection */
    public SirenQueryRewriter(Client client, String indexName, String searchPreference, String input, boolean doFullFieldDataLookup, boolean canDoSingleIndex, boolean needVisibilityOnTopLevel) {
        super(client, indexName, input, searchPreference, doFullFieldDataLookup, canDoSingleIndex, needVisibilityOnTopLevel);
    }

    @Override
    protected QueryBuilder build(ASTExpansion node) {
        ASTIndexLink link = node.getIndexLink();
        ASTIndexLink myIndex = metadataManager.getMyIndex();
        FilterBuilder filter;

        if (link == myIndex && !node.isGenerated()) {
            return super.build(node);
        } else {
            if ("(null)".equals(link.getLeftFieldname()))
                return super.build(node);

            if (_isBuildingAggregate)
                return matchAllQuery();

            FilterJoinBuilder fjb = new FilterJoinBuilder(link.getLeftFieldname()).path(link.getRightFieldname()).indices(link.getIndexName());
            fjb.query(applyVisibility(build(node.getQuery()), link.getIndexName()));

            if (!doFullFieldDataLookup)
                fjb.maxTermsPerShard(1024);

            filter = fjb;
        }

        if (node.getFilterQuery() != null) {
            BoolFilterBuilder bfb = boolFilter();

            bfb.must(filter);
            bfb.must(queryFilter(build(node.getFilterQuery())));
            filter = bfb;
        }

        return filteredQuery(matchAllQuery(), filter);
    }
}
