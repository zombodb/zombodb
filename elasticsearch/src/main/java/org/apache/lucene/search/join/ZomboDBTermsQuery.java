package org.apache.lucene.search.join;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.hppc.LongSet;

import java.io.IOException;

/**
 * A query that has an array of terms from a specific field. This query will match documents have one or more terms in
 * the specified field that match with the terms specified in the array.
 */
public class ZomboDBTermsQuery extends MultiTermQuery {

    private final LongSet terms;
    private final Query fromQuery; // Used for equals() only

    ZomboDBTermsQuery(String field, Query fromQuery, LongSet terms) {
        super(field);
        this.fromQuery = fromQuery;
        this.terms = terms;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        if (this.terms.size() == 0) {
            return TermsEnum.EMPTY;
        }

        return new SeekingTermSetTermsEnum(terms.iterator(null), this.terms);
    }

    @Override
    public String toString(String string) {
        return "ZomboDBTermsQuery{" + "field=" + field + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj) || getClass() != obj.getClass())
            return false;

        ZomboDBTermsQuery other = (ZomboDBTermsQuery) obj;
        return fromQuery.equals(other.fromQuery);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result += prime * fromQuery.hashCode();
        return result;
    }

    private static class SeekingTermSetTermsEnum extends FilteredTermsEnum {

        private final LongSet terms;

        SeekingTermSetTermsEnum(TermsEnum tenum, LongSet terms) {
            super(tenum);
            this.terms = terms;
        }

        @Override
        protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
            return new BytesRef();
        }

        @Override
        protected AcceptStatus accept(BytesRef term) throws IOException {
            long value = NumericUtils.prefixCodedToLong(term);

            if (terms.contains(value)) {
                return AcceptStatus.YES;
            } else {
                return AcceptStatus.NO;
            }
        }
    }
}
