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
import com.carrotsearch.hppc.LongArrayList;
import llc.zombodb.fast_terms.FastTermsAction;
import llc.zombodb.fast_terms.FastTermsResponse;
import llc.zombodb.utils.IntArrayMergeSortIterator;
import llc.zombodb.utils.LongArrayMergeSortIterator;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.transport.TransportClient;

import java.util.AbstractCollection;
import java.util.Iterator;

class CrossJoinQueryRewriteHelper {

    static Query rewriteQuery(TransportClient client, CrossJoinQuery crossJoin) {
        FastTermsResponse response = FastTermsAction.INSTANCE.newRequestBuilder(client)
                .setIndices(crossJoin.getIndex())
                .setTypes(crossJoin.getType())
                .setFieldname(crossJoin.getRightFieldname())
                .setQuery(crossJoin.getQuery())
                .get();

        if (response.getTotalDataCount() == 0)
            return new MatchNoDocsQuery();

        switch (response.getDataType()) {
            case INT:
                return buildRangeOrSetQuery(crossJoin.getLeftFieldname(), response.getTotalDataCount(), (int[][]) response.getAllData(), response.getAllDataCounts());
            case LONG:
                return buildRangeOrSetQuery(crossJoin.getLeftFieldname(), response.getTotalDataCount(), (long[][]) response.getAllData(), response.getAllDataCounts());
            case STRING: {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (int shardId = 0; shardId < response.getSuccessfulShards(); shardId++) {
                    int count = response.getDataCount(shardId);
                    if (count > 0) {
                        final Object[] strings = response.getData(shardId);
                        int finalShardId = shardId;
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
                                return response.getDataCount(finalShardId);
                            }
                        }), BooleanClause.Occur.SHOULD);
                    }
                }
                return builder.build();
            }
            default:
                throw new RuntimeException("Unrecognized data type: " + response.getDataType());
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

    private static Query newSetQuery(String field, int count, long... values) {
        final BytesRef encoded = new BytesRef(new byte[Long.BYTES]);

        return new PointInSetQuery(field, 1, Long.BYTES,
                new PointInSetQuery.Stream() {

                    int upto;

                    @Override
                    public BytesRef next() {
                        if (upto == count) {
                            return null;
                        } else {
                            LongPoint.encodeDimension(values[upto], encoded.bytes, 0);
                            upto++;
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

    public static Query buildRangeOrSetQuery(String field, int count, long[][] values, int[] counts) {
        LongArrayMergeSortIterator itr = new LongArrayMergeSortIterator(values, counts);
        LongArrayList points = new LongArrayList(count);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        int clauses = 0;

        while (itr.hasNext()) {
            long next = itr.next();
            long head, tail;    // range bounds, inclusive
            int cnt = 0;

            head = tail = next;
            while (itr.hasNext()) {
                next = itr.next();
                if (next != tail+1) {
                    // we need 'next' for the subsequent iteration
                    itr.push(next);
                    break;
                }
                tail++;
                cnt++;
            }

            if (cnt == 0) {
                // just one value
                points.add(head);
            } else if (cnt < 2) {
                // just two consecutive values
                points.add(head);
                points.add(tail);
            } else {
                // it's a range
                if (tail-head < 100 || clauses >= BooleanQuery.getMaxClauseCount()-1) {
                    // the range is too small to care about or we have too many already
                    for (long i=head; i<=tail; i++)
                        points.add(i);
                } else {
                    builder.add(LongPoint.newRangeQuery(field, head, tail), BooleanClause.Occur.SHOULD);
                    clauses++;
                }
            }
        }

        if (points.elementsCount > 0) {
            builder.add(newSetQuery(field, points.elementsCount, points.buffer), BooleanClause.Occur.SHOULD);
        }

        return builder.build();
    }

    public static Query buildRangeOrSetQuery(String field, int count, int[][] values, int[] counts) {
        IntArrayMergeSortIterator itr = new IntArrayMergeSortIterator(values, counts);
        IntArrayList points = new IntArrayList(count);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        int clauses = 0;

        while (itr.hasNext()) {
            int next = itr.next();
            int head, tail;    // range bounds, inclusive
            int cnt = 0;

            head = tail = next;
            while (itr.hasNext()) {
                next = itr.next();
                if (next != tail+1) {
                    // we need 'next' for the subsequent iteration
                    itr.push(next);
                    break;
                }
                tail++;
                cnt++;
            }

            if (cnt == 0) {
                // just one value
                points.add(head);
            } else if (cnt < 2) {
                // just two consecutive values
                points.add(head);
                points.add(tail);
            } else {
                // it's a range
                if (tail-head < 100 || clauses >= BooleanQuery.getMaxClauseCount()-1) {
                    // the range is too small to care about or we have too many already
                    for (int i=head; i<=tail; i++)
                        points.add(i);
                } else {
                    builder.add(IntPoint.newRangeQuery(field, head, tail), BooleanClause.Occur.SHOULD);
                    clauses++;
                }
            }
        }

        if (points.elementsCount > 0) {
            builder.add(newSetQuery(field, points.elementsCount, points.buffer), BooleanClause.Occur.SHOULD);
        }

        return builder.build();
    }

}
