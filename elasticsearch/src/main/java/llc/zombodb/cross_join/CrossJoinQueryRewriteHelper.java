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
                return newSetQuery(crossJoin.getLeftFieldname(), (int[][]) response.getAllData(), response.getAllDataCounts());
            case LONG:
                return newSetQuery(crossJoin.getLeftFieldname(), (long[][]) response.getAllData(), response.getAllDataCounts());
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

    private static Query newSetQuery(String field, long[][] values, int[] counts) {
        LongArrayMergeSortIterator itr = new LongArrayMergeSortIterator(values, counts);
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

    private static Query newSetQuery(String field, int[][] values, int[] counts) {
        IntArrayMergeSortIterator itr = new IntArrayMergeSortIterator(values, counts);
        final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

        return new PointInSetQuery(field, 1, Integer.BYTES,
                new PointInSetQuery.Stream() {
                    @Override
                    public BytesRef next() {
                        if (!itr.hasNext()) {
                            return null;
                        } else {
                            IntPoint.encodeDimension(itr.next(), encoded.bytes, 0);
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
