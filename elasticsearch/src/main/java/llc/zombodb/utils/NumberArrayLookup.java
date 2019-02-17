package llc.zombodb.utils;

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.util.*;
import java.util.stream.LongStream;

/**
 * Allows quick lookup for a value in an array of longs.
 * <p>
 * If the array of values can fit within 32bits, we scale each value so we
 * can use a bitset for O(1) lookup,
 * <p>
 * Otherwise the array of values is sorted and we binary search for O(log(n)) lookup.
 */
public class NumberArrayLookup implements Streamable {

    private static class NoCopyByteArrayOutputStream extends ByteArrayOutputStream {
        NoCopyByteArrayOutputStream(int size) {
            super(size);
        }

        byte[] getBytes() {
            return buf;
        }

        int getNumBytes() {
            return count;
        }
    }

    private Roaring64NavigableMap longs;

    public static NumberArrayLookup fromStreamInput(StreamInput in) throws IOException {
        NumberArrayLookup nal = new NumberArrayLookup();
        nal.readFrom(in);
        return nal;
    }

    private NumberArrayLookup() {
        // noop
    }

    public NumberArrayLookup(Roaring64NavigableMap bitmap) {
        longs = bitmap;
    }

    /* exists for testing */
    NumberArrayLookup(long min, long max, long[] values, int many) {
        longs = new Roaring64NavigableMap(true);
        for (int i=0; i<many; i++)
            longs.addLong(values[i]);
    }


    /**
     * Is the specific value contained within?
     */
    public boolean get(long value) {
        return longs.contains(value);
    }

    public int size() {
        return longs.getIntCardinality();
    }

    public LongIterator iterator() {
        final org.roaringbitmap.longlong.LongIterator itr = longs.getLongIterator();
        return new LongIterator() {
            private long pushed;
            private boolean hasPushed;

            @Override
            public boolean hasNext() {
                return hasPushed || itr.hasNext();
            }

            @Override
            public long next() {
                if (hasPushed) {
                    hasPushed = false;
                    return pushed;
                }
                return itr.next();
            }

            @Override
            public void push(long value) {
                pushed = value;
                hasPushed = true;
            }
        };
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numbytes = in.readVInt();
        byte[] bytes = new byte[numbytes];
        in.readBytes(bytes, 0, numbytes);
        try (ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            try {
                longs = (Roaring64NavigableMap) oin.readObject();
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        NoCopyByteArrayOutputStream baos = new NoCopyByteArrayOutputStream(4096);
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(longs);
            oos.flush();
        }
        out.writeVInt(baos.getNumBytes());
        out.writeBytes(baos.getBytes(), 0, baos.getNumBytes());
    }

    @Override
    public int hashCode() {
        return longs.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        NumberArrayLookup other = (NumberArrayLookup) obj;
        return longs.equals(other.longs);
    }
}
