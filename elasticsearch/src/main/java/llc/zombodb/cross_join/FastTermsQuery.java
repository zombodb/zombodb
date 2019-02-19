package llc.zombodb.cross_join;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;

import llc.zombodb.fast_terms.FastTermsResponse;

public class FastTermsQuery extends Query {

    private final String leftFieldname;
    private final String type;
    private final String fieldType;
    private final FastTermsResponse fastTerms;
    private final boolean alwaysJoinWithDocValues;

    public FastTermsQuery(String leftFieldname, String type, String fieldType, FastTermsResponse fastTerms, boolean alwaysJoinWithDocValues) {
        this.leftFieldname = leftFieldname;
        this.type = type;
        this.fieldType = fieldType;
        this.fastTerms = fastTerms;
        this.alwaysJoinWithDocValues = alwaysJoinWithDocValues;
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
        // this condition exists only so we can exercise issue #338
        if (alwaysJoinWithDocValues)
            return this;

        Query rewritten = CrossJoinQueryRewriteHelper.rewriteQuery(leftFieldname, fastTerms);
        return rewritten == null ? this : rewritten;
    }

    @Override
    public String toString(String field) {
        return leftFieldname + "=" + fastTerms.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;

        FastTermsQuery other = (FastTermsQuery) obj;
        return Objects.equals(this.leftFieldname, other.leftFieldname) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.fieldType, other.fieldType) &&
                Objects.equals(this.fastTerms, other.fastTerms) &&
                Objects.equals(this.alwaysJoinWithDocValues, other.alwaysJoinWithDocValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftFieldname, type, fieldType, fastTerms, alwaysJoinWithDocValues);
    }
}
