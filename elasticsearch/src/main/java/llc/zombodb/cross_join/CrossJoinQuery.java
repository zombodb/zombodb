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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;

import java.io.IOException;
import java.util.Objects;

class CrossJoinQuery extends Query {

    private final String index;
    private final String type;
    private final String leftFieldname;
    private final String rightFieldname;
    private final String fieldType;
    private final int thisShardId;
    private final boolean canOptimizeJoins;
    private final FastTermsResponse fastTerms;

    CrossJoinQuery(String index, String type, String leftFieldname, String rightFieldname, boolean canOptimizeJoins, String fieldType, int thisShardId, FastTermsResponse fastTerms) {
        this.index = index;
        this.type = type;
        this.leftFieldname = leftFieldname;
        this.rightFieldname = rightFieldname;
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

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
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

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        // Using this query and the FastTermsResponse, try to rewrite into a more simple/efficient query
        Query rewritten = CrossJoinQueryRewriteHelper.rewriteQuery(this, fastTerms);

        if (rewritten != this)
            return rewritten;   // and we did!

        // nope, we gotta do it ourselves
        return this;
    }

    @Override
    public String toString(String field) {
        return "cross_join(index=" + index + ", type=" + type + ", left=" + leftFieldname + ", right=" + rightFieldname + ", shard=" + thisShardId + ", canOptimizeJoins=" + canOptimizeJoins + ", fastTerms=" + fastTerms + ")";
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
                Objects.equals(fastTerms, other.fastTerms);
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
                fastTerms);
    }
}
