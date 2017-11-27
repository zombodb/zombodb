package llc.zombodb.utils;

import com.carrotsearch.hppc.LongArrayList;
import com.zaxxer.sparsebits.SparseBitSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private List<long[]> longs;
    private List<Integer> longLengths;
    private SparseBitSet bitset;
    private int bitsetLength = -1;
    private long min;

    public static NumberArrayLookup fromStreamInput(StreamInput in) throws IOException {
        NumberArrayLookup nal = new NumberArrayLookup();
        nal.readFrom(in);
        return nal;
    }

    private NumberArrayLookup() {

    }

    public NumberArrayLookup(long min, long max) {
        long range = max - min;

        this.min = min;

        if (range >= Integer.MIN_VALUE && range < Integer.MAX_VALUE - 1) {
            // the range of longs can scale to fit within 32 bits
            // so we're free to use a bitset
            this.bitset = new SparseBitSet((int) range + 1);
            this.longs = null;
            this.longLengths = null;
        } else {
            // we have to do something else
            this.bitset = null;
            this.longs = new ArrayList<>();
            this.longLengths = new ArrayList<>();
        }
    }

    private LongArrayList ranges;

    public long[] getRanges() {
        return ranges == null ? null : ranges.buffer;
    }

    /**
     * May sort the incoming long[] of bits using {@link java.util.Arrays#sort(long[])}
     */
    public void setAll(long[] bits, int length) {
        if (bitset != null) {
            for (int i = 0; i < length; i++)
                bitset.set((int) (bits[i] - min));

            for (LongIterator itr = iterator(); itr.hasNext();) {
                long head, tail;

                head = tail = itr.next();
                while (itr.hasNext()) {
                    long next = itr.next();
                    if (next != tail+1) {
                        itr.push(next); // save for next time
                        break;
                    }
                    tail = next;
                }

                if (tail - head > 1014) {
                    // we have a range of sufficient values to care about

                    // clear bits included in this range
                    bitset.clear((int) (head-min), (int) ((tail+1)-min));

                    // record the range as two longs
                    try {
                        ranges.add(head);
                        ranges.add(tail);
                    } catch (NullPointerException npe) {
                        // first time when 'ranges' is uninitialized
                        ranges = new LongArrayList();
                        ranges.add(head);
                        ranges.add(tail);
                    }
                }
            }
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
            if (!(asint >= 0 && bitset.get(asint))) {
                // value is not in our bitset
                if (ranges != null) {
                    // scan through ranges to see if the incoming value is a member
                    for (int i = 0; i < ranges.elementsCount; i += 2) {
                        long low = ranges.buffer[i];
                        long high = ranges.buffer[i + 1];
                        if (value >= low && value <= high)
                            return true;    // found it
                    }
                }
                // value is not in any ranges
                return false;
            }
            // value is in our bitset
            return true;
        } else {
            for (int i = 0; i < longs.size(); i++) {
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
            int cnt = 1 + (ranges == null ? 0 : ranges.size()/2);
            int idx = 0;
            LongIterator[] iterators = new LongIterator[cnt];

            iterators[idx++] = new LongIterator() {
                private long pushback;
                private boolean havePushback;
                private int idx = bitset.nextSetBit(0);

                @Override
                public boolean hasNext() {
                    return idx >= 0 || havePushback;
                }

                @Override
                public long next() {
                    if (havePushback) {
                        havePushback = false;
                        return pushback;
                    } else {
                        long value = idx + min;
                        idx = bitset.nextSetBit(idx + 1);
                        return value;
                    }
                }

                @Override
                public void push(long value) {
                    pushback = value;
                    havePushback = true;
                }
            };

            if (ranges != null) {
                for (int i = 0; i < ranges.size(); i += 2) {
                    long low = ranges.get(i);
                    long high = ranges.get(i + 1);

                    iterators[idx++] = LongIterator.create(LongStream.rangeClosed(low, high).iterator());
                }
            }

            return LongIterator.create(iterators);

        } else if (longs.size() > 0) {
            long[][] arrays = new long[longs.size()][];
            int[] lengths = new int[longs.size()];
            for (int i = 0; i < arrays.length; i++) {
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
        boolean haveBitset = in.readBoolean();

        if (haveBitset) {
            min = in.readZLong();
            bitsetLength = in.readVInt();
            int numbytes = in.readVInt();
            byte[] bytes = new byte[numbytes];
            in.readBytes(bytes, 0, numbytes);
            try (ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                try {
                    bitset = (SparseBitSet) oin.readObject();
                } catch (ClassNotFoundException cnfe) {
                    throw new IOException(cnfe);
                }
            }
        } else {
            int numlongs = in.readVInt();

            longs = new ArrayList<>(numlongs);
            longLengths = new ArrayList<>(numlongs);
            for (int i = 0; i < numlongs; i++) {
                longs.add(DeltaEncoder.decode_longs_from_deltas(in));
                longLengths.add(longs.get(i).length);
            }
        }

        if (in.readBoolean()) {
            ranges = new LongArrayList();
            ranges.buffer = DeltaEncoder.decode_longs_from_deltas(in);
            ranges.elementsCount = ranges.buffer.length;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(bitset != null);

        if (bitset != null) {
            out.writeZLong(min);
            out.writeVInt(bitset.cardinality());
            NoCopyByteArrayOutputStream baos = new NoCopyByteArrayOutputStream(4096);
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(bitset);
                oos.flush();
            }
            out.writeVInt(baos.getNumBytes());
            out.writeBytes(baos.getBytes(), 0, baos.getNumBytes());
        } else {
            out.writeVInt(longs.size());
            for (int i = 0; i < longs.size(); i++)
                DeltaEncoder.encode_longs_as_deltas(longs.get(i), longLengths.get(i), out);
        }

        out.writeBoolean(ranges != null);
        if (ranges != null) {
            DeltaEncoder.encode_longs_as_deltas(ranges.buffer, ranges.elementsCount, out);
        }
    }
}
