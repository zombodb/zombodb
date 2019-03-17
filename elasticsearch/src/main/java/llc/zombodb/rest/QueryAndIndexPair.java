/*
 * Copyright 2013-2015 Technology Concepts & Design, Inc
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
package llc.zombodb.rest;

import llc.zombodb.query_parser.ASTLimit;
import org.elasticsearch.index.query.QueryBuilder;

public class QueryAndIndexPair {
    private final QueryBuilder query;
    private final QueryBuilder visibilityFilter;
    private final String indexName;
    private final ASTLimit limit;
    private final boolean wantScores;
    private final String dump;

    public QueryAndIndexPair(QueryBuilder query, QueryBuilder visibilityFilter, String indexName, ASTLimit limit, boolean wantScores, String dump) {
        this.query = query;
        this.visibilityFilter = visibilityFilter;
        this.indexName = indexName;
        this.limit = limit;
        this.wantScores = wantScores;
        this.dump = dump;
    }

    public QueryBuilder getQueryBuilder() {
        return query;
    }

    public QueryBuilder getVisibilityFilter() {
        return visibilityFilter;
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean hasLimit() {
        return limit != null;
    }

    public ASTLimit getLimit() {
        return limit;
    }

    public boolean wantScores() {
        return wantScores;
    }

    public String getDump() {
        return dump;
    }
}
