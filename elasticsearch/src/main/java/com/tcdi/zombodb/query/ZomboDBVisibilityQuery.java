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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class ZomboDBVisibilityQuery extends Query {

    private final long myXid;
    private final long xmin;
    private final long xmax;
    private final int commandid;
    private final Set<Long> activeXids;

    ZomboDBVisibilityQuery(long myXid, long xmin, long xmax, int commandid, long[] activeXids) {
        this.myXid = myXid;
        this.xmin = xmin;
        this.xmax = xmax;
        this.commandid = commandid;
        this.activeXids = new HashSet<>();
        for (long xid : activeXids)
            this.activeXids.add(xid);
    }

    @Override
    public Weight createWeight(final IndexSearcher searcher, boolean needsScores) throws IOException {
        return new ConstantScoreWeight(this) {
            Map<Integer, FixedBitSet> visibilityBitSets;

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                if (visibilityBitSets == null)
                    visibilityBitSets = VisibilityQueryHelper.determineVisibility(myXid, xmin, xmax, commandid, activeXids, searcher);
                FixedBitSet bitset = visibilityBitSets.get(context.ord);
                return bitset == null ? null : new ConstantScoreScorer(this, 0, new BitDocIdSet(bitset).iterator());
            }
        };
    }

    @Override
    public String toString(String field) {
        return "visibility(myXid=" + myXid + ", xmin=" + xmin + ", xmax=" + xmax + ", commandid=" + commandid + ", active=" + activeXids + ")";
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash = hash * 31 + (int)(myXid ^ (myXid >>> 32));
        hash = hash * 31 + (int)(xmin ^ (xmin >>> 32));
        hash = hash * 31 + (int)(xmax ^ (xmax >>> 32));
        hash = hash * 31 + (commandid);
        hash = hash * 31 + activeXids.hashCode();
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
                this.activeXids.containsAll(eq.activeXids) && this.activeXids.size() == eq.activeXids.size();
    }
}
