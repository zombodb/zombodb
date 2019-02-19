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
import java.util.List;
import java.util.Objects;
import java.util.PrimitiveIterator;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;

import llc.zombodb.utils.CompactHashSet;
import llc.zombodb.utils.NumberBitmap;

public class FastTermsResponse extends BroadcastResponse implements StatusToXContentObject {
    public enum DataType {
        NONE,
        INT,
        LONG,
        STRING
    }

    private DataType dataType;

    private NumberBitmap numbers;
    private CompactHashSet strings;

    public FastTermsResponse() {

    }

    public FastTermsResponse(StreamInput in) throws IOException {
        readFrom(in);
    }

    FastTermsResponse(int shardCount, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, DataType dataType) {
        super(shardCount, successfulShards, failedShards, shardFailures);

        this.dataType = dataType;
        numbers = new NumberBitmap();
        strings = new CompactHashSet();
    }

    void addData(Object data) {
        switch (dataType) {
            case INT:
            case LONG:
                numbers.merge((NumberBitmap) data);
                break;
            case STRING:
                strings.addAll((CompactHashSet) data);
                break;
        }
    }

    public DataType getDataType() {
        return dataType;
    }

    public NumberBitmap getNumbers() {
        return numbers;
    }

    public long estimateByteSize() {
        switch (dataType) {
            case NONE:
                return 0;

            case INT:
            case LONG: {
                return numbers.estimateByteSize();
            }

            case STRING: {
                long size = strings.size()*2;
                for (String s : strings)
                    size += s.length()*2;
                return size;
            }

            default:
                throw new RuntimeException("Unexpected data type: " + dataType);
        }
    }

    public PrimitiveIterator.OfLong getNumbersIterator() {
        return numbers.iterator();
    }

    public int getDocCount() {
        switch (dataType) {
            case INT:
            case LONG:
                return numbers.size();

            case STRING:
                return strings.size();

            default:
                throw new RuntimeException("Unexpected data type: " + dataType);
        }
    }

    public CompactHashSet getStrings() {
        return strings;
    }

    public String[] getSortedStrings() {
        return strings.stream().sorted().toArray(String[]::new);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        dataType = in.readEnum(DataType.class);
        switch (dataType) {
            case INT:
            case LONG: {
                numbers = new NumberBitmap(in);
                break;
            }

            case STRING: {
                strings = new CompactHashSet(in);
                break;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeEnum(dataType);

        switch (dataType) {
            case INT:
            case LONG: {
                numbers.writeTo(out);
                break;
            }

            case STRING: {
                strings.writeTo(out);
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
        return Objects.hash(dataType, numbers, strings);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        FastTermsResponse other = (FastTermsResponse) obj;
        return Objects.equals(dataType, other.dataType) &&
                Objects.equals(numbers, other.numbers) &&
                Objects.equals(strings, other.strings);
    }

    public FastTermsResponse throwShardFailure() {
        if (getFailedShards() > 0) {
            // if there was at least one failure, report and re-throw the first
            // Note that even after we walk down to the original cause, the stacktrace is already lost.
            Throwable cause = getShardFailures()[0].getCause();
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            throw new RuntimeException(cause);
        }

        return this;
    }

}
