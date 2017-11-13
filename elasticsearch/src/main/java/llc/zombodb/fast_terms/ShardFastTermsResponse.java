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

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;

public class ShardFastTermsResponse extends BroadcastShardResponse {
    private FastTermsResponse.DataType dataType;
    private int dataCount;
    private int[] ints;
    private long[] longs;
    private Object[] strings;

    public ShardFastTermsResponse() {

    }

    public ShardFastTermsResponse(ShardId shardId, FastTermsResponse.DataType dataType, Object data, int dataCount) {
        super(shardId);
        this.dataType = dataType;
        this.dataCount = dataCount;
        switch (dataType) {
            case INT:
                ints = (int[]) data;
                Arrays.sort(ints, 0, dataCount);
                break;
            case LONG:
                longs = (long[]) data;
                Arrays.sort(longs, 0, dataCount);
                break;
            case STRING:
                strings = (Object[]) data;
                Arrays.sort(strings, 0, dataCount, (o1, o2) -> {
                    String a = (String) o1;
                    String b = (String) o2;
                    return a.compareTo(b);
                });
                break;
        }
    }

    public FastTermsResponse.DataType getDataType() {
        return dataType;
    }

    public int getDataCount() {
        return dataCount;
    }

    public <T> T getData() {
        switch (dataType) {
            case INT:
                return (T) ints;
            case LONG:
                return (T) longs;
            case STRING:
                return (T) strings;
            default:
                throw new RuntimeException("Unrecognized data type: " + dataType);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        dataType = in.readEnum(FastTermsResponse.DataType.class);
        switch (dataType) {
            case INT:
                ints = DeltaEncoder.decode_ints_from_deltas(in);
                dataCount = ints.length;
                break;
            case LONG:
                longs = DeltaEncoder.decode_longs_from_deltas(in);
                dataCount = longs.length;
                break;
            case STRING:
                strings = in.readStringArray();
                dataCount = strings.length;
                break;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(dataType);
        switch (dataType) {
            case INT:
                DeltaEncoder.encode_ints_as_deltas(ints, dataCount, out);
                break;
            case LONG:
                DeltaEncoder.encode_longs_as_deltas(longs, dataCount, out);
                break;
            case STRING:
                write_strings(out);
                break;
        }
    }

    private void write_strings(StreamOutput out) throws IOException {
        out.writeVInt(dataCount);
        for (int i=0; i<dataCount; i++)
            out.writeString(String.valueOf(strings[i]));
    }

}
