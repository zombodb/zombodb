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

import com.carrotsearch.hppc.IntArrayList;
import llc.zombodb.fast_terms.FastTermsResponse;
import llc.zombodb.utils.LongIterator;
import llc.zombodb.utils.NumberArrayLookupMergeSortIterator;
import llc.zombodb.utils.PushbackIterator;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.util.*;

class CrossJoinQueryRewriteHelper {
    static class Range {
        private long start, end;

        public Range(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }


    static Query rewriteQuery(CrossJoinQuery crossJoin) {
        FastTermsResponse fastTerms = crossJoin.fastTerms;

        if (fastTerms.getTotalDataCount() == 0)
            return new MatchNoDocsQuery();
        else if (fastTerms.getPointCount() >= 50_000)
            return crossJoin;   // 50k points is about the break-even point in terms of performance v/s just scanning all DocValues

        //
        // rewrite the provided CrossJoinQuery into a series of point and/or range queries
        //
        switch (fastTerms.getDataType()) {
            case INT:
                return buildIntRangeOrSetQuery(crossJoin.getLeftFieldname(), new NumberArrayLookupMergeSortIterator(fastTerms.getNumberLookup()));
            case LONG:
                return buildLongRangeOrSetQuery(crossJoin.getLeftFieldname(), new NumberArrayLookupMergeSortIterator(fastTerms.getNumberLookup()), fastTerms.getRanges());
            case STRING: {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (int shardId = 0; shardId < fastTerms.getNumShards(); shardId++) {
                    int count = fastTerms.getStringCount(shardId);
                    if (count > 0) {
                        final Object[] strings = fastTerms.getStrings(shardId);
                        builder.add(new TermInSetQuery(crossJoin.getLeftFieldname(), new AbstractCollection<BytesRef>() {
                            @Override
                            public Iterator<BytesRef> iterator() {
                                return new Iterator<BytesRef>() {
                                    int idx = 0;

                                    @Override
                                    public boolean hasNext() {
                                        return idx < count;
                                    }

                                    @Override
                                    public BytesRef next() {
                                        return new BytesRef(String.valueOf(strings[idx++]));
                                    }
                                };
                            }

                            @Override
                            public int size() {
                                return count;
                            }
                        }), BooleanClause.Occur.SHOULD);
                    }
                }
                return builder.build();
            }
            default:
                throw new RuntimeException("Unrecognized data type: " + fastTerms.getDataType());
        }
    }

    private static Query newSetQuery(String field, int count, int... values) {
        final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

        return new PointInSetQuery(field, 1, Integer.BYTES,
                new PointInSetQuery.Stream() {

                    int upto;

                    @Override
                    public BytesRef next() {
                        if (upto == count) {
                            return null;
                        } else {
                            IntPoint.encodeDimension(values[upto], encoded.bytes, 0);
                            upto++;
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

    private static Query newSetQuery(String field, LongIterator itr) {
        final BytesRef encoded = new BytesRef(new byte[Long.BYTES]);

        return new PointInSetQuery(field, 1, Long.BYTES,
                new PointInSetQuery.Stream() {

                    @Override
                    public BytesRef next() {
                        if (!itr.hasNext()) {
                            return null;
                        } else {
                            LongPoint.encodeDimension(itr.next(), encoded.bytes, 0);
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

    private static Query buildLongRangeOrSetQuery(String field, NumberArrayLookupMergeSortIterator itr, Collection<long[]> ranges) {
        List<Range> rangeList = new ArrayList<>();

        for (long[] range : ranges) {
            for (int i = 0; i < range.length; i += 2)
                rangeList.add(new Range(range[i], range[i + 1]));
        }

        // sort lowest to highest and merge together adjacent ranges
        rangeList.sort((o1, o2) -> Long.compare(o1.end, o2.start));

        for (PushbackIterator<Range> i = new PushbackIterator<>(rangeList.iterator()); i.hasNext(); ) {
            Range a = i.next();
            if (!i.hasNext())
                break;
            Range b = i.next();

            if (a.end + 1 == b.start) {
                // these two ranges can be merged
                a.end = b.end;

                // and we don't need 'b' anymore
                i.remove();

                // but we want to see if 'a' can merge with the next range
                i.push(a);
            }
        }

        List<Query> clauses = new ArrayList<>();
        for (Range range : rangeList) {
            clauses.add(LongPoint.newRangeQuery(field, range.start, range.end));
        }

        if (itr.hasNext()) {
            clauses.add(newSetQuery(field, itr));
        }

        return buildQuery(clauses);
    }

    private static Query buildIntRangeOrSetQuery(String field, NumberArrayLookupMergeSortIterator itr) {
        IntArrayList points = new IntArrayList();
        List<Query> clauses = new ArrayList<>();

        while (itr.hasNext()) {
            int head, tail;    // range bounds, inclusive
            int next = (int) itr.next();

            head = tail = next;
            while (itr.hasNext()) {
                next = (int) itr.next();
                if (next != tail + 1) {
                    // we need 'next' for the subsequent iteration
                    itr.push(next);
                    break;
                } else if (tail == next) {
                    // it's a duplicate value, so we can de-dup it
                    continue;
                }
                tail++;
            }

            if (head == tail) {
                // just one value
                points.add(head);
            } else if (tail - head == 1) {
                // just two points, not worth making a range
                points.add(head);
                points.add(tail);
            } else if (clauses.size() >= BooleanQuery.getMaxClauseCount() - 1) {
                // we have too many range clauses already
                for (int i = head; i <= tail; i++)
                    points.add(i);
            } else {
                // it's a range
                clauses.add(IntPoint.newRangeQuery(field, head, tail));
            }
        }

        if (points.size() > 0) {
            clauses.add(newSetQuery(field, points.size(), points.buffer));
        }

        return buildQuery(clauses);
    }

    private static Query buildQuery(List<Query> clauses) {
        if (clauses.size() == 1) {
            return clauses.get(0);
        } else {
            BooleanQuery.Builder top;
            BooleanQuery.Builder builder = top = new BooleanQuery.Builder();
            int cnt = 0;
            for (Query q : clauses) {
                if (cnt++ >= BooleanQuery.getMaxClauseCount()-1) {
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
