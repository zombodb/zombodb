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

import java.io.IOException;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import llc.zombodb.fast_terms.collectors.FastTermsCollector;
import llc.zombodb.utils.CompactHashSet;
import llc.zombodb.utils.NumberBitmap;

public class ShardFastTermsResponse extends BroadcastShardResponse {

    private FastTermsResponse.DataType dataType;
    private int dataCount;
    private NumberBitmap bitset;
    private CompactHashSet strings;

    ShardFastTermsResponse() {
        super();
    }

    ShardFastTermsResponse(ShardId shardId) {
        super(shardId);
    }

    public ShardFastTermsResponse(ShardId shardId, FastTermsResponse.DataType dataType, FastTermsCollector collector) {
        super(shardId);
        assert dataType != null;

        this.dataType = dataType;
        this.dataCount = collector.getDataCount();
        switch (dataType) {
            case INT:
            case LONG:
                bitset = (NumberBitmap) collector.getData();
                break;
            case STRING:
                strings = (CompactHashSet) collector.getData();
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
        if (in.readBoolean()) {
            dataType = in.readEnum(FastTermsResponse.DataType.class);
            switch (dataType) {
                case INT:
                case LONG:
                    if (in.readBoolean()) {
                        bitset = new NumberBitmap(in);
                        dataCount = bitset.size();
                    }
                    break;
                case STRING:
                    if (in.readBoolean()) {
                        strings = new CompactHashSet(in);
                        dataCount = strings.size();
                    }
                    break;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(dataType != null);
        if (dataType != null) {
            out.writeEnum(dataType);
            switch (dataType) {
                case INT:
                case LONG:
                    out.writeBoolean(bitset != null);
                    if (bitset != null) {
                        bitset.writeTo(out);
                    }
                    break;
                case STRING:
                    out.writeBoolean(strings != null);
                    if (strings != null) {
                        strings.writeTo(out);
                    }
                    break;
            }
        }
    }

}
