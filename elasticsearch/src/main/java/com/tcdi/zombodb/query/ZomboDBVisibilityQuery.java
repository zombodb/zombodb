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
package com.tcdi.zombodb.query;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.hppc.IntObjectMap;
import org.elasticsearch.common.lang3.ArrayUtils;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;

import java.io.IOException;
import java.util.Set;

class ZomboDBVisibilityQuery extends Query {

    private final String fieldname;
    private final long myXid;
    private final long xmin;
    private final long xmax;
    private final Set<Long> activeXids;
    private final Set<Long> committedXids;
    private final Query subquery;

    ZomboDBVisibilityQuery(String fieldname, long myXid, long xmin, long xmax, Set<Long> activeXids, Set<Long> committedXids, Query subquery) {
        this.fieldname = fieldname;
        this.myXid = myXid;
        this.xmin = xmin;
        this.xmax = xmax;
        this.activeXids = activeXids;
        this.committedXids = committedXids;
        this.subquery = subquery;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        final IntObjectMap<FixedBitSet> visibilityBitSets = VisibilityQueryHelper.determineVisibility(fieldname, subquery, myXid, xmin, xmax, activeXids, committedXids, reader);

        return new ConstantScoreQuery(
                new Filter() {
                    @Override
                    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                        return visibilityBitSets.get(context.ord);
                    }
                }
        );
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        subquery.extractTerms(terms);
    }

    @Override
    public String toString(String field) {
        return "visibility(" + fieldname + ", myXid=" + myXid + ", xmin=" + xmin + ", xmax=" + xmax + ", active=" + ArrayUtils.toString(activeXids) + ", committed=" + ArrayUtils.toString(committedXids) + ", query=" + subquery.toString(field) + ")";
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = hash * 31 + fieldname.hashCode();
        hash = hash * 31 + subquery.hashCode();
        hash = hash * 31 + (int)(myXid ^ (myXid >>> 32));
        hash = hash * 31 + (int)(xmin ^ (xmin >>> 32));
        hash = hash * 31 + (int)(xmax ^ (xmax >>> 32));
        hash = hash * 31 + ArrayUtils.toString(activeXids).hashCode();
        hash = hash * 31 + ArrayUtils.toString(committedXids).hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        assert obj instanceof ZomboDBVisibilityQuery;
        ZomboDBVisibilityQuery eq = (ZomboDBVisibilityQuery) obj;

        return this.fieldname.equals(eq.fieldname) &&
                this.subquery.equals(eq.subquery) &&
                this.myXid == eq.myXid &&
                this.xmin == eq.xmin &&
                this.xmax == eq.xmax &&
                ArrayUtils.isEquals(this.activeXids, eq.activeXids) &&
                ArrayUtils.isEquals(this.committedXids, eq.committedXids);
    }
}
