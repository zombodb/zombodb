package llc.zombodb.utils;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.PrimitiveIterator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class NumberBitmap implements Streamable {
    private Roaring64NavigableMap longs;

    private static Roaring64NavigableMap make64bitBitmap() {
        // IMPORTANT:  we must have signed longs here, so the ctor arg must be 'true'
        // Otherwise we won't maintain sorting the way we need
        return new Roaring64NavigableMap(true);
    }

    public NumberBitmap(StreamInput ois) throws IOException {
        readFrom(ois);
    }

    public NumberBitmap() {
        longs = make64bitBitmap();
    }

    public void add(long value) {
        longs.addLong(value);
    }

    public boolean contains(long value) {
        return longs.contains(value);
    }

    public int size() {
        return longs.getIntCardinality();
    }

    public PrimitiveIterator.OfLong iterator() {
        return new PrimitiveIterator.OfLong() {
            final LongIterator itr = longs.getLongIterator();

            @Override
            public boolean hasNext() {
                return itr.hasNext();
            }

            @Override
            public long nextLong() {
                return itr.next();
            }
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(longs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        NumberBitmap other = (NumberBitmap) obj;
        return Objects.equals(this.longs, other.longs);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataOutput dataOutput = new DataOutputStream(out);
        longs.runOptimize();
        longs.serialize(dataOutput);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        DataInput dataInput = new DataInputStream(in);
        longs = make64bitBitmap();
        longs.deserialize(dataInput);
    }

    public long estimateByteSize() {
        return longs.serializedSizeInBytes();
    }

    public void merge(NumberBitmap bitmap) {
        longs.or(bitmap.longs);
    }
}
