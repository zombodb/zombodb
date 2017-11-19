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

import llc.zombodb.fast_terms.collectors.FastTermsCollector;
import llc.zombodb.utils.NumberArrayLookup;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;

public class ShardFastTermsResponse extends BroadcastShardResponse {
    private FastTermsResponse.DataType dataType;
    private int dataCount;
    private NumberArrayLookup bitset;
    private Object[] strings;

    public ShardFastTermsResponse() {

    }

    public ShardFastTermsResponse(ShardId shardId, FastTermsResponse.DataType dataType, FastTermsCollector collector) {
        super(shardId);
        this.dataType = dataType;
        this.dataCount = collector.getDataCount();
        switch (dataType) {
            case INT:
            case LONG:
                bitset = new NumberArrayLookup(collector.getMin(), collector.getMax());
                bitset.setAll((long[]) collector.getData(), collector.getDataCount());
                break;
            case STRING:
                strings = (Object[]) collector.getData();
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
                return (T) bitset;
            case LONG:
                return (T) bitset;
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
                bitset = NumberArrayLookup.fromStreamInput(in);
                break;
            case LONG:
                bitset = NumberArrayLookup.fromStreamInput(in);
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
                bitset.writeTo(out);
                break;
            case LONG:
                bitset.writeTo(out);
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
