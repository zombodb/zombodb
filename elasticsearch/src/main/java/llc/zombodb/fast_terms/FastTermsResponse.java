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
        for (int i=0; i<super.getSuccessfulShards(); i++) {
            switch (dataType) {
                case INT:
                    ints[i] = in.readVIntArray();
                    break;
                case LONG:
                    longs[i] = in.readVLongArray();
                    break;
                case STRING:
                    strings[i] = in.readStringArray();
                    break;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(dataType);
        out.writeIntArray(counts);
        for (int i=0; i<super.getSuccessfulShards(); i++) {
            switch (dataType) {
                case INT:
                    out.writeVInt(counts[i]);
                    for (int j=0; j<counts[i]; j++)
                        out.writeVInt(ints[i][j]);
                    break;
                case LONG:
                    out.writeVInt(counts[i]);
                    for (int j=0; j<counts[i]; j++)
                        out.writeVLong(longs[i][j]);
                    break;
                case STRING:
                    out.writeVInt(counts[i]);
                    for (int j=0; j<counts[i]; j++)
                        out.writeString(String.valueOf(strings[i][j]));
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
