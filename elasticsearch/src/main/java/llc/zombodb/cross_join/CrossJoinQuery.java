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
package llc.zombodb.cross_join;

import java.io.IOException;
import java.util.Objects;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import llc.zombodb.fast_terms.FastTermsAction;
import llc.zombodb.fast_terms.FastTermsResponse;

class CrossJoinQuery extends Query {
    // a two-level cache for caching FastTermResponse objects so we can avoid re-executing them if the IndexSearcher
    // from which they were created is still alive.  Even then, we only want to keep things cached for 1 minute
    private static final Cache<IndexSearcher, Cache<String, FastTermsResponse>> CACHE = CacheBuilder.<IndexSearcher, Cache<String, FastTermsResponse>>builder()
            .setExpireAfterAccess(TimeValue.timeValueMinutes(1))
            .setExpireAfterWrite(TimeValue.timeValueMinutes(1))
            .build();

    private final String index;
    private final String type;
    private final String leftFieldname;
    private final String rightFieldname;
    private final String fieldType;
    private final int thisShardId;
    private final boolean canOptimizeJoins;
    private final boolean alwaysJoinWithDocValues;
    private final QueryBuilder query;
    private final Client client;
    private transient FastTermsResponse fastTerms;
    private transient Weight weight;
    private transient boolean didRewrite;

    CrossJoinQuery(String index, String type, String leftFieldname, String rightFieldname, boolean canOptimizeJoins, boolean alwaysJoinWithDocValues, String fieldType, int thisShardId, QueryBuilder query, Client client, FastTermsResponse fastTerms) {
        this.index = index;
        this.type = type;
        this.leftFieldname = leftFieldname;
        this.rightFieldname = rightFieldname;
        this.fieldType = fieldType;
        this.thisShardId = thisShardId;
        this.canOptimizeJoins = canOptimizeJoins;
        this.alwaysJoinWithDocValues = alwaysJoinWithDocValues;
        this.query = query;
        this.client = client;
        this.fastTerms = fastTerms;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getLeftFieldname() {
        return leftFieldname;
    }

    public String getRightFieldname() {
        return rightFieldname;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) {
        return new ConstantScoreWeight(this) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                if (fastTerms == null) {
                    // first resolve any nested CrossJoinQueries we might have in 'this.query' from the bottom up
                    // and it's important we resolve them from the bottom up so that we're not doing any
                    // re-entrant queries back into Elasticsearch as it'll deadlock if we exhaust the GENERIC threadpool
                    resolveNestedCrossJoins(searcher);

                    // resolve this CrossJoinQuery using FastTerms
                    fastTerms = getFastTerms(searcher, index, type, rightFieldname, query, canOptimizeJoins);
                }

                // this condition exists only so we can exercise issue #338
                if (!alwaysJoinWithDocValues) {
                    if (!didRewrite) {
                        didRewrite = true;

                        // attempt to rewrite the query into something less complicated
                        Query rewritten = CrossJoinQueryRewriteHelper.rewriteQuery(CrossJoinQuery.this, fastTerms);
                        if (rewritten != CrossJoinQuery.this)
                            weight = new ConstantScoreQuery(rewritten).createWeight(searcher, needsScores);
                    }

                    if (weight != null)
                        return weight.scorer(context);
                }

                // we have to do it the hard way by grovelling through doc values
                BitSet bitset = CrossJoinQueryExecutor.execute(
                        context,
                        type,
                        leftFieldname,
                        fieldType,
                        fastTerms
                );

                return bitset == null ? null : new ConstantScoreScorer(this, 0, new BitDocIdSet(bitset).iterator());
            }
        };
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return super.rewrite(reader);
    }

    private Stack<CrossJoinQueryBuilder> buildCrossJoinQueryBuilderStack(QueryBuilder query, Stack<CrossJoinQueryBuilder> stack) {
        if (query instanceof CrossJoinQueryBuilder) {
            CrossJoinQueryBuilder cjqb = (CrossJoinQueryBuilder) query;

            stack.push(cjqb);
            buildCrossJoinQueryBuilderStack(cjqb.query, stack);

        } else if (query instanceof BoolQueryBuilder) {
            BoolQueryBuilder bqb = (BoolQueryBuilder) query;

            for (QueryBuilder builder : bqb.must())
                buildCrossJoinQueryBuilderStack(builder, stack);
            for (QueryBuilder builder : bqb.should())
                buildCrossJoinQueryBuilderStack(builder, stack);
            for (QueryBuilder builder : bqb.mustNot())
                buildCrossJoinQueryBuilderStack(builder, stack);
            for (QueryBuilder builder : bqb.filter())
                buildCrossJoinQueryBuilderStack(builder, stack);
        } else if (query instanceof ConstantScoreQueryBuilder) {
            buildCrossJoinQueryBuilderStack(((ConstantScoreQueryBuilder) query).innerQuery(), stack);
        }

        return stack;
    }

    private void resolveNestedCrossJoins(IndexSearcher searcher) {
        Stack<CrossJoinQueryBuilder> stack = buildCrossJoinQueryBuilderStack(this.query, new Stack<>());

        // resolve all FastTerms from nested CrossJoinQueryBuilders from the bottom up
        while (!stack.isEmpty()) {
            CrossJoinQueryBuilder cjqb = stack.pop();

            if (cjqb.fastTerms == null) {
                cjqb.fastTerms(getFastTerms(searcher, cjqb.index, cjqb.type, cjqb.rightFieldname, cjqb.query, false));

                if (cjqb.fastTerms.getFailedShards() > 0)
                    throw new RuntimeException("Shard failures while executing FastTermsAction", cjqb.fastTerms.getShardFailures()[0].getCause());
            }
        }
    }

    private FastTermsResponse getFastTerms(IndexSearcher searcher, String index, String type, String rightFieldname, QueryBuilder query, boolean canOptimizeJoins) {
        try {
            String key = (canOptimizeJoins ? thisShardId : -1) + ":" + index + ":" + type + ":" + rightFieldname + ":" + query;

            FastTermsResponse response = CACHE.computeIfAbsent(searcher,
                    loader -> CacheBuilder.<String, FastTermsResponse>builder()
                            .setExpireAfterAccess(TimeValue.timeValueMinutes(1))
                            .setExpireAfterWrite(TimeValue.timeValueMinutes(1))
                            .build())
                    .computeIfAbsent(key,
                            fastTerms -> FastTermsAction.INSTANCE.newRequestBuilder(client)
                                    .setIndices(index)
                                    .setTypes(type)
                                    .setFieldname(rightFieldname)
                                    .setQuery(query)
                                    .setSourceShard(canOptimizeJoins ? thisShardId : -1)
                                    .get(TimeValue.timeValueSeconds(300))).throwShardFailure();

            return response;
        } catch (ExecutionException ee) {
            throw new RuntimeException(ee);
        }
    }

    @Override
    public String toString(String field) {
        return "cross_join(index=" + index + ", type=" + type + ", left=" + leftFieldname + ", right=" + rightFieldname + ", shard=" + thisShardId + ", canOptimizeJoins=" + canOptimizeJoins + ", query=" + query + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;

        CrossJoinQuery other = (CrossJoinQuery) obj;
        return Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(leftFieldname, other.leftFieldname) &&
                Objects.equals(rightFieldname, other.rightFieldname) &&
                Objects.equals(thisShardId, other.thisShardId) &&
                Objects.equals(canOptimizeJoins, other.canOptimizeJoins) &&
                Objects.equals(query, other.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                index,
                type,
                leftFieldname,
                rightFieldname,
                thisShardId,
                canOptimizeJoins,
                query);
    }
}
