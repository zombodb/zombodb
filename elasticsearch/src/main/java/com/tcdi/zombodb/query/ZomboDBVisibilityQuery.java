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
package com.tcdi.zombodb.query;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lang3.ArrayUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ZomboDBVisibilityQuery extends Query {

    private final Query query;
    private final String fieldname;
    private final long myXid;
    private final long xmin;
    private final long xmax;
    private final boolean all;
    private final Set<Long> activeXids;

    ZomboDBVisibilityQuery(Query query, String fieldname, long myXid, long xmin, long xmax, boolean all, Set<Long> activeXids) {
        this.query = query;
        this.fieldname = fieldname;
        this.myXid = myXid;
        this.xmin = xmin;
        this.xmax = xmax;
        this.all = all;
        this.activeXids = activeXids;
    }

    @Override
    public Query rewrite(final IndexReader reader) throws IOException {
        class VisFilter extends Filter {
            private Map<Integer, FixedBitSet> visibilityBitSets = null;
            private final IndexSearcher searcher;
            private final List<BytesRef> updatedCtids;

            private VisFilter(IndexSearcher searcher, List<BytesRef> updatedCtids) {
                this.searcher = searcher;
                this.updatedCtids = updatedCtids;
            }

            @Override
            public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                if (visibilityBitSets == null)
                    visibilityBitSets = VisibilityQueryHelper.determineVisibility(query, fieldname, myXid, xmin, xmax, all, activeXids, searcher, updatedCtids);
                return visibilityBitSets.get(context.ord);
            }
        }

        IndexSearcher searcher = new IndexSearcher(reader);
        return new XConstantScoreQuery(new VisFilter(searcher, VisibilityQueryHelper.findUpdatedCtids(searcher)));
    }

    @Override
    public void extractTerms(Set<Term> terms) {

    }

    @Override
    public String toString(String field) {
        return "visibility(" + fieldname + ", query=" + query + ", myXid=" + myXid + ", xmin=" + xmin + ", xmax=" + xmax + ", all=" + all + ", active=" + ArrayUtils.toString(activeXids) + ")";
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = hash * 31 + query.hashCode();
        hash = hash * 31 + fieldname.hashCode();
        hash = hash * 31 + (int)(myXid ^ (myXid >>> 32));
        hash = hash * 31 + (int)(xmin ^ (xmin >>> 32));
        hash = hash * 31 + (int)(xmax ^ (xmax >>> 32));
        hash = hash * 31 + (all?1:0);
        hash = hash * 31 + ArrayUtils.toString(activeXids).hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        assert obj instanceof ZomboDBVisibilityQuery;
        ZomboDBVisibilityQuery eq = (ZomboDBVisibilityQuery) obj;

        return this.query.equals(eq.query) &&
                this.fieldname.equals(eq.fieldname) &&
                this.myXid == eq.myXid &&
                this.xmin == eq.xmin &&
                this.xmax == eq.xmax &&
                this.all == eq.all &&
                ArrayUtils.isEquals(this.activeXids, eq.activeXids);
    }
}
