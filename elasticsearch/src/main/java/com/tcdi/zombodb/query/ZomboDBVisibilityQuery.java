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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lang3.ArrayUtils;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

class ZomboDBVisibilityQuery extends Query {

    private final long myXid;
    private final long xmin;
    private final long xmax;
    private final int commandid;
    private final Set<Long> activeXids;

    ZomboDBVisibilityQuery(long myXid, long xmin, long xmax, int commandid, Set<Long> activeXids) {
        this.myXid = myXid;
        this.xmin = xmin;
        this.xmax = xmax;
        this.commandid = commandid;
        this.activeXids = activeXids;
    }

    @Override
    public Query rewrite(final IndexReader reader) throws IOException {
        class VisFilter extends Filter {
            private Map<Integer, FixedBitSet> visibilityBitSets = null;
            private final IndexSearcher searcher;

            private VisFilter(IndexSearcher searcher) {
                this.searcher = searcher;
            }

            @Override
            public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                if (visibilityBitSets == null)
                    visibilityBitSets = VisibilityQueryHelper.determineVisibility(myXid, xmin, xmax, commandid, activeXids, searcher);
                return visibilityBitSets.get(context.ord);
            }
        }

        IndexSearcher searcher = new IndexSearcher(reader);
        return new XConstantScoreQuery(new VisFilter(searcher));
    }

    @Override
    public void extractTerms(Set<Term> terms) {

    }

    @Override
    public String toString(String field) {
        return "visibility(myXid=" + myXid + ", xmin=" + xmin + ", xmax=" + xmax + ", commandid=" + commandid + ", active=" + ArrayUtils.toString(activeXids) + ")";
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = hash * 31 + (int)(myXid ^ (myXid >>> 32));
        hash = hash * 31 + (int)(xmin ^ (xmin >>> 32));
        hash = hash * 31 + (int)(xmax ^ (xmax >>> 32));
        hash = hash * 31 + (commandid);
        hash = hash * 31 + ArrayUtils.toString(activeXids).hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        assert obj instanceof ZomboDBVisibilityQuery;
        ZomboDBVisibilityQuery eq = (ZomboDBVisibilityQuery) obj;

        return
                this.myXid == eq.myXid &&
                this.xmin == eq.xmin &&
                this.xmax == eq.xmax &&
                this.commandid == eq.commandid &&
                ArrayUtils.isEquals(this.activeXids, eq.activeXids);
    }
}
