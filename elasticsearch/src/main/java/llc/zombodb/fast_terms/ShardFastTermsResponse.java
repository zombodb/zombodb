package llc.zombodb.fast_terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;

public class ShardFastTermsResponse extends BroadcastShardResponse {
    private FastTermsResponse.DataType dataType;
    private boolean hasNegative;
    private int dataCount;
    private int[] ints;
    private long[] longs;
    private Object[] strings;

    public ShardFastTermsResponse() {

    }

    public ShardFastTermsResponse(ShardId shardId, FastTermsResponse.DataType dataType, Object data, boolean hasNegative, int dataCount, boolean doSorting) {
        super(shardId);
        this.dataType = dataType;
        this.hasNegative = hasNegative;
        this.dataCount = dataCount;
        switch (dataType) {
            case INT:
                ints = (int[]) data;
                if (doSorting)
                    Arrays.sort(ints, 0, dataCount);
                break;
            case LONG:
                longs = (long[]) data;
                if (doSorting)
                    Arrays.sort(longs, 0, dataCount);
                break;
            case STRING:
                strings = (Object[]) data;
                if (doSorting)
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

    public boolean hasNegative() {
        return hasNegative;
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
                dataCount = ints.length;
                break;
            case LONG:
                hasNegative = in.readBoolean();
                if (hasNegative) {
                    longs = new long[in.readVInt()];
                    for (int i=0; i<longs.length; i++)
                        longs[i] = in.readZLong();
                } else {
                    longs = in.readVLongArray();
                }
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
        out.writeBoolean(hasNegative);
        out.writeVInt(dataCount);
        if (hasNegative) {
            for (int i = 0; i < dataCount; i++)
                out.writeZLong(longs[i]);
        } else {
            for (int i = 0; i < dataCount; i++)
                out.writeVLong(longs[i]);
        }
    }

    private void write_strings(StreamOutput out) throws IOException {
        out.writeVInt(dataCount);
        for (int i=0; i<dataCount; i++)
            out.writeString(String.valueOf(strings[i]));
    }

}
