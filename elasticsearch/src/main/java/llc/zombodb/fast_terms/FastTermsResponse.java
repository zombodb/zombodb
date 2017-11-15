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
package llc.zombodb.fast_terms;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import llc.zombodb.utils.IntArrayMergeSortIterator;
import llc.zombodb.utils.LongArrayMergeSortIterator;
import llc.zombodb.utils.StringArrayMergeSortIterator;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;

public class FastTermsResponse extends BroadcastResponse implements StatusToXContentObject {
    public enum DataType {
        INT,
        LONG,
        STRING
    }

    private DataType dataType;
    private int[][] ints;
    private long[][] longs;
    private Object[][] strings;
    private LongArrayList longArray;
    private IntArrayList intArray;
    private ObjectArrayList<String> stringArray;

    private int[] lengths;

    public FastTermsResponse() {

    }

    FastTermsResponse(int shardCount, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, DataType dataType) {
        super(shardCount, successfulShards, failedShards, shardFailures);
        this.dataType = dataType;
        if (dataType != null) {
            lengths = new int[shardCount];
            switch (dataType) {
                case INT:
                    ints = new int[shardCount][];
                    break;
                case LONG:
                    longs = new long[shardCount][];
                    break;
                case STRING:
                    strings = new Object[shardCount][];
                    break;
            }
        }
    }

    void addData(int shardId, Object data, int count) {
        lengths[shardId] = count;
        switch (dataType) {
            case INT:
                ints[shardId] = (int[]) data;
                break;
            case LONG:
                longs[shardId] = (long[]) data;
                break;
            case STRING:
                strings[shardId] = (Object[]) data;
                break;
        }
    }

    public DataType getDataType() {
        return dataType;
    }

    public <T> T getAllData() {
        switch (dataType) {
            case INT:
                return (T) ints;
            case LONG:
                return (T) longs;
            case STRING:
                return (T) strings;
            default:
                throw new RuntimeException("Unrecognized type: " + dataType);
        }
    }

    public <T> T getData(int shard) {
        switch (dataType) {
            case INT:
                return (T) ints[shard];
            case LONG:
                return (T) longs[shard];
            case STRING:
                return (T) strings[shard];
            default:
                throw new RuntimeException("Unrecognized type: " + dataType);
        }
    }

    public int[] getAllDataLengths() {
        return lengths;
    }

    public int getDataCount(int shard) {
        return lengths[shard];
    }

    public int getTotalDataCount() {
        int total = 0;
        for (int cnt : lengths)
            total += cnt;
        return total;
    }

    public synchronized LongArrayList getLongArray() {
        if (longArray == null) {
            LongArrayMergeSortIterator sorter = new LongArrayMergeSortIterator(longs, lengths);
            longArray = new LongArrayList(sorter.getTotal());
            while (sorter.hasNext())
                longArray.add(sorter.next());
            longs = null;
        }

        return longArray;
    }

    public synchronized IntArrayList getIntArray() {
        if (intArray == null) {
            IntArrayMergeSortIterator sorter = new IntArrayMergeSortIterator(ints, lengths);
            intArray = new IntArrayList(sorter.getTotal());
            while (sorter.hasNext())
                intArray.add(sorter.next());
            ints = null;
        }

        return intArray;
    }
    
    public synchronized ObjectArrayList<String> getStringArray() {
        if (stringArray == null) {
            StringArrayMergeSortIterator sorter = new StringArrayMergeSortIterator(strings, lengths);
            stringArray = new ObjectArrayList<String>(sorter.getTotal());
            while (sorter.hasNext())
                stringArray.add(sorter.next());
            strings = null;
        }

        return stringArray;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        dataType = in.readEnum(DataType.class);
        lengths = in.readIntArray();
        switch (dataType) {
            case INT:
                ints = new int[lengths.length][];
                break;
            case LONG:
                longs = new long[lengths.length][];
                break;
            case STRING:
                strings = new Object[lengths.length][];
                break;
        }

        for (int shardId=0; shardId<super.getSuccessfulShards(); shardId++) {
            switch (dataType) {
                case INT:
                    ints[shardId] = DeltaEncoder.decode_ints_from_deltas(in);
                    break;
                case LONG:
                    longs[shardId] = DeltaEncoder.decode_longs_from_deltas(in);
                    break;
                case STRING:
                    strings[shardId] = in.readStringArray();
                    break;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(dataType);
        out.writeIntArray(lengths);
        for (int shardId=0; shardId<super.getSuccessfulShards(); shardId++) {
            switch (dataType) {
                case INT:
                    DeltaEncoder.encode_ints_as_deltas(ints[shardId], lengths[shardId], out);
                    break;
                case LONG:
                    DeltaEncoder.encode_longs_as_deltas(longs[shardId], lengths[shardId], out);
                    break;
                case STRING:
                    out.writeVInt(lengths[shardId]);
                    for (int i = 0; i< lengths[shardId]; i++)
                        out.writeString(String.valueOf(strings[shardId][i]));
                    break;
            }
        }
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(getSuccessfulShards(), getTotalShards(), getShardFailures());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(builder, params, getTotalShards(), getSuccessfulShards(), 0, getFailedShards(), getShardFailures());
        builder.endObject();
        return builder;
    }
}
