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
    private boolean[] negatives;

    public FastTermsResponse() {

    }

    FastTermsResponse(int shardCount, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, DataType dataType) {
        super(shardCount, successfulShards, failedShards, shardFailures);
        this.dataType = dataType;
        if (dataType != null) {
            counts = new int[shardCount];
            negatives = new boolean[shardCount];
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

    void addData(int shardId, Object data, boolean hasNegative, int count) {
        counts[shardId] = count;
        negatives[shardId] = hasNegative;
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

    public boolean hasNegative(int shard) {
        return negatives[shard];
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
                negatives = new boolean[counts.length];
                longs = new long[counts.length][];
                break;
            case STRING:
                strings = new Object[counts.length][];
                break;
        }

        for (int shardId=0; shardId<super.getSuccessfulShards(); shardId++) {
            switch (dataType) {
                case INT:
                    ints[shardId] = in.readVIntArray();
                    break;
                case LONG:
                    negatives[shardId] = in.readBoolean();
                    if (negatives[shardId]) {
                        longs[shardId] = new long[in.readVInt()];
                        for (int i=0; i<longs[shardId].length; i++)
                            longs[shardId][i] = in.readZLong();
                    } else {
                        longs[shardId] = in.readVLongArray();
                    }
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
                    out.writeVInt(counts[shardId]);
                    for (int i=0; i<counts[shardId]; i++)
                        out.writeVInt(ints[shardId][i]);
                    break;
                case LONG:
                    out.writeBoolean(negatives[shardId]);
                    out.writeVInt(counts[shardId]);

                    if (negatives[shardId]) {
                        for (int i = 0; i < counts[shardId]; i++)
                            out.writeZLong(longs[shardId][i]);
                    } else {
                        for (int i = 0; i < counts[shardId]; i++)
                            out.writeVLong(longs[shardId][i]);
                    }
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
