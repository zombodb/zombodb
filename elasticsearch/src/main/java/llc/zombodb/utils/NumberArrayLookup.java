package llc.zombodb.utils;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.*;
import java.util.*;

/**
 * Allows quick lookup for a value in an array of longs.
 *
 * If the array of values can fit within 32bits, we scale each value so we
 * can use a bitset for O(1) lookup,
 *
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

    private List<long[]> longs;
    private List<Integer> longLengths;
    private BitSet bitset;
    private int bitsetLength = -1;
    private long min;

    public static NumberArrayLookup fromStreamInput(StreamInput in) throws IOException {
        NumberArrayLookup sbs = new NumberArrayLookup();
        sbs.readFrom(in);
        return sbs;
    }

    private NumberArrayLookup() {

    }

    public NumberArrayLookup(long min, long max) {
        long range = max - min;

        this.min = min;

        if (range >= Integer.MIN_VALUE && range < Integer.MAX_VALUE-1) {
            // the range of longs can scale to fit within 32 bits
            // so we're free to use a bitset
            this.bitset = new BitSet((int) range+1);
            this.longs = null;
            this.longLengths = null;
        } else {
            // we have to do something else
            this.bitset = null;
            this.longs = new ArrayList<>();
            this.longLengths = new ArrayList<>();
        }
    }

    /**
     * May sort the incoming long[] of bits using {@link java.util.Arrays#sort(long[])}
     */
    public void setAll(long[] bits, int length) {
        if (bitset != null) {
            for (int i = 0; i < length; i++)
                bitset.set((int) (bits[i] - min));
        } else {
            Arrays.sort(bits, 0, length);
            longs.add(bits);
            longLengths.add(length);
        }
    }

    /**
     * Is the specific value contained within?
     */
    public boolean get(long value) {
        if (bitset != null) {
            int asint = (int) (value - min);
            return asint >= 0 && bitset.get(asint);
        } else {
            for (int i=0; i<longs.size(); i++) {
                int length = longLengths.get(i);
                if (Arrays.binarySearch(longs.get(i), 0, length, value) >= 0)
                    return true;
            }
            return false;
        }
    }

    public int getValueCount() {
        if (bitset != null) {
            if (bitsetLength == -1)
                return bitsetLength = bitset.cardinality();
            else
                return bitsetLength;
        } else {
            int total = 0;
            for (int length : longLengths)
                total += length;
            return total;
        }
    }

    public LongIterator iterator() {
        if (bitset != null) {
            PrimitiveIterator.OfInt itr = bitset.stream().iterator();
            return new LongIterator() {
                private long pushbash;
                private boolean havePushback;
                @Override
                public boolean hasNext() {
                    return itr.hasNext() || havePushback;
                }

                @Override
                public long next() {
                    try {
                        return havePushback ? pushbash : itr.nextInt() + min;
                    } finally {
                        havePushback = false;
                    }
                }

                @Override
                public void push(long value) {
                    pushbash = value;
                    havePushback = true;
                }
            };
        } else if (longs.size() > 0) {
            long[][] arrays = new long[longs.size()][];
            int[] lengths = new int[longs.size()];
            for (int i=0; i<arrays.length; i++) {
                arrays[i] = longs.get(i);
                lengths[i] = longLengths.get(i);
            }
            return new LongArrayMergeSortIterator(arrays, lengths);
        }

        // we have nothing to return so just return an empty iterator
        return new LongIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public long next() {
                return 0;
            }

            @Override
            public void push(long value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        min = in.readZLong();
        boolean haveBitset = in.readBoolean();

        if (haveBitset) {
            bitsetLength = in.readVInt();
            int numbytes = in.readVInt();
            byte[] bytes = new byte[numbytes];
            in.readBytes(bytes, 0, numbytes);
            try (ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                try {
                    bitset = (BitSet) oin.readObject();
                } catch (ClassNotFoundException cnfe) {
                    throw new IOException(cnfe);
                }
            }
        } else {
            int numlongs = in.readVInt();

            longs = new ArrayList<>(numlongs);
            longLengths = new ArrayList<>(numlongs);
            for (int i=0; i<numlongs; i++) {
                longs.add(DeltaEncoder.decode_longs_from_deltas(in));
                longLengths.add(longs.get(i).length);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZLong(min);
        out.writeBoolean(bitset != null);

        if (bitset != null) {
            out.writeVInt(bitset.cardinality());
            NoCopyByteArrayOutputStream baos = new NoCopyByteArrayOutputStream(4096);
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(bitset);
                oos.flush();
                out.writeVInt(baos.getNumBytes());
                out.writeBytes(baos.getBytes(), 0, baos.getNumBytes());
            }
        } else {
            out.writeVInt(longs.size());
            for (int i=0; i<longs.size(); i++)
                DeltaEncoder.encode_longs_as_deltas(longs.get(i), longLengths.get(i), out);
        }
    }
}
