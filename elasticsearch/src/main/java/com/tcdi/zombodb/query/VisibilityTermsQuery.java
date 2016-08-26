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

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.hppc.LongSet;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Set;

/**
 * A query that has an array of terms from a specific field. This query will match documents have one or more terms in
 * the specified field that match with the terms specified in the array.
 */
class VisibilityTermsQuery extends MultiTermQuery {

    private final OpenBitSet terms;

    // these are used for equals() only
    private final long xmin;
    private final long xmax;
    private final Set<Long> activeXids;
    private final Query fromQuery;

    VisibilityTermsQuery(String field, long xmin, long xmax, Set<Long> activeXids, Query fromQuery, OpenBitSet terms) {
        super(field);
        this.xmin = xmin;
        this.xmax = xmax;
        this.activeXids = activeXids;
        this.fromQuery = fromQuery;
        this.terms = terms;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        if (this.terms.cardinality() == 0) {
            return TermsEnum.EMPTY;
        }

        return new SeekingTermSetTermsEnum(terms.iterator(null));
    }

    @Override
    public String toString(String string) {
        return "VisibilityTermsQuery{" + "field=" + field + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj) || getClass() != obj.getClass())
            return false;

        VisibilityTermsQuery other = (VisibilityTermsQuery) obj;
        return fromQuery.equals(other.fromQuery) &&
                xmin == other.xmin &&
                xmax == other.xmax &&
                activeXids.equals(other.activeXids);
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = hash * 31 + fromQuery.hashCode();
        hash = hash * 31 + (int)(xmin ^ (xmin >>> 32));
        hash = hash * 31 + (int)(xmax ^ (xmax >>> 32));
        hash = hash * 31 + activeXids.hashCode();
        return hash;
    }

    private class SeekingTermSetTermsEnum extends FilteredTermsEnum {

        SeekingTermSetTermsEnum(TermsEnum tenum) {
            super(tenum);
        }

        @Override
        protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
            return new BytesRef();
        }

        @Override
        protected AcceptStatus accept(BytesRef term) throws IOException {
            long value = NumericUtils.prefixCodedToLong(term);
            return terms.get(value) ? AcceptStatus.YES : AcceptStatus.NO;
        }
    }
}
