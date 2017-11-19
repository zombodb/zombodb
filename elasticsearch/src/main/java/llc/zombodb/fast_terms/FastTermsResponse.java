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

import com.carrotsearch.hppc.ObjectArrayList;
import llc.zombodb.utils.NumberArrayLookup;
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
    private int numShards;

    private NumberArrayLookup[] lookups;
    private Object[][] strings;
    private int[] numStrings;
    private ObjectArrayList<String> stringArray;

    public FastTermsResponse() {

    }

    FastTermsResponse(int shardCount, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, DataType dataType) {
        super(shardCount, successfulShards, failedShards, shardFailures);
        this.dataType = dataType;
        this.numShards = successfulShards;
        if (dataType != null) {
            switch (dataType) {
                case INT:
                    lookups = new NumberArrayLookup[successfulShards];
                    break;
                case LONG:
                    lookups = new NumberArrayLookup[successfulShards];
                    break;
                case STRING:
                    strings = new Object[successfulShards][];
                    numStrings = new int[successfulShards];
                    break;
            }
        }
    }

    void addData(int shardId, Object data, int count) {
        if (shardId > numShards)
            numShards = shardId;
        switch (dataType) {
            case INT:
                lookups[shardId] = (NumberArrayLookup) data;
                break;
            case LONG:
                lookups[shardId] = (NumberArrayLookup) data;
                break;
            case STRING:
                strings[shardId] = (Object[]) data;
                numStrings[shardId] = count;
                break;
        }
    }

    public DataType getDataType() {
        return dataType;
    }

    public NumberArrayLookup[] getNumberLookup() {
        return lookups;
    }

    public int getTotalDataCount() {
        switch (dataType) {
            case INT:
            case LONG: {
                int total = 0;
                for (NumberArrayLookup bitset : lookups) {
                    total +=  bitset.getValueCount();
                }
                return total;
            }

            case STRING: {
                int total = 0;
                for (int cnt : numStrings)
                    total += cnt;
                return total;
            }

            default:
                throw new RuntimeException("Unexpected data type: " + dataType);
        }
    }


    public synchronized ObjectArrayList<String> getStringArray() {
        if (stringArray == null) {
            StringArrayMergeSortIterator sorter = new StringArrayMergeSortIterator(strings, numStrings);
            stringArray = new ObjectArrayList<String>(sorter.getTotal());
            while (sorter.hasNext())
                stringArray.add(sorter.next());
            strings = null;
        }

        return stringArray;
    }

    public Object[] getStrings(int shardId) {
        return strings[shardId];
    }

    public int getStringCount(int shardId) {
        return numStrings[shardId];
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        numShards = in.readVInt();
        dataType = in.readEnum(DataType.class);
        switch (dataType) {
            case INT:
                lookups = new NumberArrayLookup[numShards];
                break;
            case LONG:
                lookups = new NumberArrayLookup[numShards];
                break;
            case STRING:
                strings = new Object[numShards][];
                numStrings = new int[numShards];
                break;
        }

        for (int shardId=0; shardId<super.getSuccessfulShards(); shardId++) {
            switch (dataType) {
                case INT:
                    lookups[shardId] = NumberArrayLookup.fromStreamInput(in);
                    break;
                case LONG:
                    lookups[shardId] = NumberArrayLookup.fromStreamInput(in);
                    break;
                case STRING:
                    strings[shardId] = in.readStringArray();
                    numStrings[shardId] = strings[shardId].length;
                    break;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numShards);
        out.writeEnum(dataType);
        for (int shardId=0; shardId<super.getSuccessfulShards(); shardId++) {
            switch (dataType) {
                case INT:
                    lookups[shardId].writeTo(out);
                    break;
                case LONG:
                    lookups[shardId].writeTo(out);
                    break;
                case STRING:
                    out.writeVInt(numStrings[shardId]);
                    for (int i = 0; i< numStrings[shardId]; i++)
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
