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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;

import llc.zombodb.fast_terms.FastTermsResponse;
import llc.zombodb.utils.NumberArrayLookup;

class CrossJoinQueryRewriteHelper {

    interface RangeAndSetQueryCreator {
        Query newSetQuery(String field, PrimitiveIterator.OfLong itr);
    }

    static class LongRangeAndSetQueryCreator implements RangeAndSetQueryCreator {
        @Override
        public Query newSetQuery(String field, PrimitiveIterator.OfLong itr) {
            final BytesRef encoded = new BytesRef(new byte[Long.BYTES]);

            return new PointInSetQuery(field, 1, Long.BYTES,
                    new PointInSetQuery.Stream() {

                        @Override
                        public BytesRef next() {
                            if (!itr.hasNext()) {
                                return null;
                            } else {
                                LongPoint.encodeDimension(itr.nextLong(), encoded.bytes, 0);
                                return encoded;
                            }
                        }
                    }) {
                @Override
                protected String toString(byte[] value) {
                    assert value.length == Long.BYTES;
                    return Long.toString(LongPoint.decodeDimension(value, 0));
                }
            };
        }
    }

    static class IntRangeAndSetQueryCreator implements RangeAndSetQueryCreator {

        @Override
        public Query newSetQuery(String field, PrimitiveIterator.OfLong itr) {
            final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

            return new PointInSetQuery(field, 1, Integer.BYTES,
                    new PointInSetQuery.Stream() {

                        @Override
                        public BytesRef next() {
                            if (!itr.hasNext()) {
                                return null;
                            } else {
                                IntPoint.encodeDimension((int) itr.nextLong(), encoded.bytes, 0);
                                return encoded;
                            }
                        }
                    }) {
                @Override
                protected String toString(byte[] value) {
                    assert value.length == Integer.BYTES;
                    return Integer.toString(IntPoint.decodeDimension(value, 0));
                }
            };
        }
    }


    static Query rewriteQuery(String leftFieldname, FastTermsResponse fastTerms) {

        if (fastTerms.getDocCount() == 0)
            return new MatchNoDocsQuery();
        else if (fastTerms.getDocCount() > 50_000)
            return null;   // 50k points is about the break-even point in terms of performance v/s just scanning all DocValues

        //
        // rewrite the provided CrossJoinQuery into a series of point and/or range queries
        //
        switch (fastTerms.getDataType()) {
            case INT:
                return buildRangeOrSetQuery(leftFieldname, fastTerms.getNumberLookup(), new IntRangeAndSetQueryCreator());
            case LONG:
                return buildRangeOrSetQuery(leftFieldname, fastTerms.getNumberLookup(), new LongRangeAndSetQueryCreator());
            case STRING: {
                final String[] sortedStrings = fastTerms.getSortedStrings();

                if (sortedStrings.length == 0)
                    return new MatchNoDocsQuery();

                return new TermInSetQuery(leftFieldname, new AbstractCollection<BytesRef>() {
                    @Override
                    public Iterator<BytesRef> iterator() {
                        return new Iterator<BytesRef>() {
                            int idx = 0;

                            @Override
                            public boolean hasNext() {
                                return idx < sortedStrings.length;
                            }

                            @Override
                            public BytesRef next() {
                                return new BytesRef(String.valueOf(sortedStrings[idx++]));
                            }
                        };
                    }

                    @Override
                    public int size() {
                        return sortedStrings.length;
                    }
                });
            }

            default:
                throw new RuntimeException("Unrecognized data type: " + fastTerms.getDataType());
        }
    }

    private static Query buildRangeOrSetQuery(String field, NumberArrayLookup lookups, RangeAndSetQueryCreator queryCreator) {
        List<Query> clauses = new ArrayList<>();

        for (PrimitiveIterator.OfLong itr : lookups.iterators()) {
            if (itr.hasNext())
                clauses.add(queryCreator.newSetQuery(field, itr));
        }

        return clauses.isEmpty() ? new MatchNoDocsQuery() : buildQuery(clauses);
    }

    private static Query buildQuery(List<Query> clauses) {
        if (clauses.size() == 1) {
            return clauses.get(0);
        } else {
            BooleanQuery.Builder top;
            BooleanQuery.Builder builder = top = new BooleanQuery.Builder();
            int cnt = 0;
            for (Query q : clauses) {
                if (cnt++ >= BooleanQuery.getMaxClauseCount() - 1) {
                    BooleanQuery.Builder tmp = new BooleanQuery.Builder();
                    tmp.add(builder.build(), BooleanClause.Occur.SHOULD);
                    builder = tmp;
                } else {
                    builder.add(q, BooleanClause.Occur.SHOULD);
                }
            }
            return top.build();
        }
    }

}
