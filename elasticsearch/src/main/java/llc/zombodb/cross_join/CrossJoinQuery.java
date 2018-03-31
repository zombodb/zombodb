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

import llc.zombodb.fast_terms.FastTermsResponse;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Objects;

class CrossJoinQuery extends Query {

    private final String index;
    private final String type;
    private final String leftFieldname;
    private final String rightFieldname;
    private final QueryBuilder query;
    private final String fieldType;
    private final int thisShardId;
    private final boolean canOptimizeJoins;
    private final FastTermsResponse fastTerms;

    public CrossJoinQuery(String index, String type, String leftFieldname, String rightFieldname, QueryBuilder query, boolean canOptimizeJoins, String fieldType, int thisShardId, FastTermsResponse fastTerms) {
        this.index = index;
        this.type = type;
        this.leftFieldname = leftFieldname;
        this.rightFieldname = rightFieldname;
        this.query = query;
        this.fieldType = fieldType;
        this.thisShardId = thisShardId;
        this.canOptimizeJoins = canOptimizeJoins;
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

    public QueryBuilder getQuery() {
        return query;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        // Run the FastTerms action to get the doc key values that we need for joining

        if (fastTerms.getFailedShards() > 0)
            throw new IOException(fastTerms.getShardFailures()[0].getCause());

        // Using this query and the FastTermsResponse, try to rewrite into a more simple/efficient query
        Query rewritten = CrossJoinQueryRewriteHelper.rewriteQuery(this, fastTerms);

        if (rewritten != this) {
            // during rewriting, we were given a new query, so use that to create weights
            return rewritten.createWeight(searcher, needsScores);
        } else {
            // otherwise we need to do it ourselves
            return new ConstantScoreWeight(this) {

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
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
    }

    @Override
    public String toString(String field) {
        return "cross_join(index=" + index + ", type=" + type + ", left=" + leftFieldname + ", right=" + rightFieldname + ", query=" + query + ", shard=" + thisShardId + ", canOptimizeJoins=" + canOptimizeJoins + ")";
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
                Objects.equals(query, other.query) &&
                Objects.equals(thisShardId, other.thisShardId) &&
                Objects.equals(canOptimizeJoins, other.canOptimizeJoins);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, leftFieldname, rightFieldname, query, thisShardId, canOptimizeJoins);
    }
}
