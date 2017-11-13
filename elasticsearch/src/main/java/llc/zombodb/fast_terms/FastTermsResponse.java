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

    private int[] counts;

    public FastTermsResponse() {

    }

    FastTermsResponse(int shardCount, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, DataType dataType) {
        super(shardCount, successfulShards, failedShards, shardFailures);
        this.dataType = dataType;
        if (dataType != null) {
            counts = new int[shardCount];
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
        counts[shardId] = count;
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

    public int[] getAllDataCounts() {
        return counts;
    }

    public int getDataCount(int shard) {
        return counts[shard];
    }

    public int getTotalDataCount() {
        int total = 0;
        for (int cnt : counts)
            total += cnt;
        return total;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        dataType = in.readEnum(DataType.class);
        counts = in.readIntArray();
        switch (dataType) {
            case INT:
                ints = new int[counts.length][];
                break;
            case LONG:
                longs = new long[counts.length][];
                break;
            case STRING:
                strings = new Object[counts.length][];
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
        out.writeIntArray(counts);
        for (int shardId=0; shardId<super.getSuccessfulShards(); shardId++) {
            switch (dataType) {
                case INT:
                    DeltaEncoder.encode_ints_as_deltas(ints[shardId], counts[shardId], out);
                    break;
                case LONG:
                    DeltaEncoder.encode_longs_as_deltas(longs[shardId], counts[shardId], out);
                    break;
                case STRING:
                    out.writeVInt(counts[shardId]);
                    for (int i=0; i<counts[shardId]; i++)
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
