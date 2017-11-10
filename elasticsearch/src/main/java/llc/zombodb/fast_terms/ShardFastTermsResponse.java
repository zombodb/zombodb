package llc.zombodb.fast_terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ShardFastTermsResponse extends BroadcastShardResponse {
    private FastTermsResponse.DataType dataType;
    private int dataCount;
    private int[] ints;
    private long[] longs;
    private Object[] strings;

    public ShardFastTermsResponse() {

    }

    public ShardFastTermsResponse(String index, ShardId shardId, FastTermsResponse.DataType dataType, Object data, int dataCount) {
        super(shardId);
        this.dataType = dataType;
        this.dataCount = dataCount;
        switch (dataType) {
            case INT:
                ints = (int[]) data;
                break;
            case LONG:
                longs = (long[]) data;
                break;
            case STRING:
                strings = (Object[]) data;
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
                ints = in.readVIntArray();
                break;
            case LONG:
                longs = in.readVLongArray();
                break;
            case STRING:
                strings = in.readStringArray();
                break;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(dataType);
        switch (dataType) {
            case INT:
                write_ints(out);
                break;
            case LONG:
                write_longs(out);
                break;
            case STRING:
                write_strings(out);
                break;
        }
    }

    private void write_ints(StreamOutput out) throws IOException {
        out.writeVInt(dataCount);
        for (int i=0; i<dataCount; i++)
            out.writeVInt(ints[i]);
    }

    private void write_longs(StreamOutput out) throws IOException {
        out.writeVInt(dataCount);
        for (int i=0; i<dataCount; i++)
            out.writeVLong(longs[i]);
    }

    private void write_strings(StreamOutput out) throws IOException {
        out.writeVInt(dataCount);
        for (int i=0; i<dataCount; i++)
            out.writeString(String.valueOf(strings[i]));
    }

}
