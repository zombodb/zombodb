package llc.zombodb.cross_join;

import llc.zombodb.fast_terms.FastTermsAction;
import llc.zombodb.fast_terms.FastTermsResponse;
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

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int i = 0; i < response.getSuccessfulShards(); i++) {
            int count = response.getDataCount(i);
            if (count > 0) {
                switch (response.getDataType()) {
                    case INT:
                        builder.add(newSetQuery(crossJoin.getLeftFieldname(), count, (int[]) response.getData(i)), BooleanClause.Occur.SHOULD);
                        break;

                    case LONG:
                        builder.add(newSetQuery(crossJoin.getLeftFieldname(), count, (long[]) response.getData(i)), BooleanClause.Occur.SHOULD);
                        break;

                    case STRING: {
                        final int shardId = i;
                        final Object[] strings = response.getData(shardId);
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
                                return response.getDataCount(shardId);
                            }
                        }), BooleanClause.Occur.SHOULD);
                    }
                    break;

                    default:
                        throw new RuntimeException("Unrecognized data type: " + response.getDataType());
                }
            }
        }

        return builder.build();
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


}
