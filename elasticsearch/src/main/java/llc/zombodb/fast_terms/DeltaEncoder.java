package llc.zombodb.fast_terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DeltaEncoder {

    public static void encode_longs_as_deltas(long[] longs, int len, StreamOutput out) throws IOException {
        out.writeVInt(len);
        if (len > 0) {
            boolean hasNegative = longs[0] < 0;
            out.writeBoolean(hasNegative);

            if (hasNegative) {
                out.writeZLong(longs[0]);
                for (int i = 1; i < len; i++) {
                    out.writeZLong(longs[i] - longs[i - 1]);
                }
            } else {
                out.writeVLong(longs[0]);
                for (int i = 1; i < len; i++) {
                    out.writeVLong(longs[i] - longs[i - 1]);
                }
            }
        }
    }

    public static long[] decode_longs_from_deltas(StreamInput in) throws IOException {
        int len = in.readVInt();
        long[] longs = new long[len];

        if (len > 0) {
            boolean hasNegative = in.readBoolean();

            if (hasNegative) {
                longs[0] = in.readZLong();
                for (int i = 1; i < len; i++) {
                    longs[i] = longs[i - 1] + in.readZLong();
                }
            } else {
                longs[0] = in.readVLong();
                for (int i = 1; i < len; i++) {
                    longs[i] = longs[i - 1] + in.readVLong();
                }
            }
        }
        return longs;
    }

    public static void encode_ints_as_deltas(int[] ints, int len, StreamOutput out) throws IOException {
        out.writeVInt(len);
        if (len > 0) {
            out.writeVInt(ints[0]);
            for (int i = 1; i < len; i++) {
                out.writeVInt(ints[i] - ints[i - 1]);
            }
        }
    }

    public static int[] decode_ints_from_deltas(StreamInput in) throws IOException {
        int len = in.readVInt();
        int[] ints = new int[len];

        if (len > 0) {
            ints[0] = in.readVInt();
            for (int i = 1; i < len; i++) {
                ints[i] = ints[i - 1] + in.readVInt();
            }
        }
        return ints;
    }
}
