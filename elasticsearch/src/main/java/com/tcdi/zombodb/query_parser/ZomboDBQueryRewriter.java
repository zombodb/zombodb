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
    protected void performCustomOptimizations() {
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
