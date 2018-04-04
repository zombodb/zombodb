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
import java.util.*;

public class FastTermsResponse extends BroadcastResponse implements StatusToXContentObject {
    public enum DataType {
        NONE,
        INT,
        LONG,
        STRING
    }

    private String index;
    private DataType dataType = DataType.NONE;
    private int numShards;

    private NumberArrayLookup[] lookups = new NumberArrayLookup[0];
    private Object[][] strings = new Object[0][];
    private int[] numStrings = new int[0];
    private ObjectArrayList<String> stringArray;

    public FastTermsResponse() {

    }

    public FastTermsResponse(StreamInput in) throws IOException {
        readFrom(in);
    }

    FastTermsResponse(String index, int shardCount, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, DataType dataType) {
        super(shardCount, successfulShards, failedShards, shardFailures);
        assert dataType != null;

        this.index = index;
        this.dataType = dataType;
        this.numShards = shardCount;

        switch (dataType) {
            case INT:
            case LONG:
                lookups = new NumberArrayLookup[shardCount];
                break;
            case STRING:
                strings = new Object[shardCount][];
                numStrings = new int[shardCount];
                break;
        }
    }

    void addData(int shardId, Object data, int count) {
        switch (dataType) {
            case INT:
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

    public int getNumShards() {
        return numShards;
    }

    public NumberArrayLookup[] getNumberLookup() {
        return lookups;
    }

    public Collection<long[]> getRanges() {
        List<long[]> rc = new ArrayList<>();
        for (NumberArrayLookup nal : lookups) {
            long[] ranges = nal.getRanges();
            if (ranges != null) {
                rc.add(ranges);
            }
        }

        return rc;
    }

    public int getPointCount() {
        if (dataType == DataType.NONE)
            return 0;

        switch (dataType) {
            case INT:
            case LONG: {
                int total = 0;
                for (NumberArrayLookup nal : lookups) {
                    int len = nal.getCountOfBits();
                    total += len > 0 ? len : nal.getCountOfLongs();
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

    public int getTotalDataCount() {
        if (dataType == DataType.NONE)
            return 0;

        switch (dataType) {
            case INT:
            case LONG: {
                int total = 0;
                for (NumberArrayLookup nal : lookups) {
                    total +=  nal.getValueCount();
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

        index = in.readString();
        numShards = in.readVInt();
        dataType = in.readEnum(DataType.class);
        switch (dataType) {
            case INT:
            case LONG:
                lookups = new NumberArrayLookup[numShards];
                break;
            case STRING:
                strings = new Object[numShards][];
                numStrings = new int[numShards];
                break;
        }

        for (int shardId=0; shardId<numShards; shardId++) {
            switch (dataType) {
                case INT:
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

        out.writeString(index);
        out.writeVInt(numShards);
        out.writeEnum(dataType);
        for (int shardId=0; shardId<numShards; shardId++) {
            switch (dataType) {
                case INT:
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

    @Override
    public int hashCode() {
        int hash = Objects.hash(index, dataType, numShards, Arrays.deepHashCode(lookups),
                Arrays.deepHashCode(strings), stringArray);
        hash = 31 * hash + Arrays.hashCode(numStrings);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        FastTermsResponse other = (FastTermsResponse) obj;
        return Objects.equals(index, other.index) &&
                Objects.equals(dataType, other.dataType) &&
                Objects.equals(numShards, other.numShards) &&
                Objects.deepEquals(lookups, other.lookups) &&
                Objects.deepEquals(strings, other.strings) &&
                Objects.deepEquals(numStrings, other.numStrings) &&
                Objects.equals(stringArray, other.stringArray);
    }

}
