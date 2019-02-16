package llc.zombodb.utils;

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

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

    public static class StreamableLongScatterSet extends LongScatterSet implements Streamable {

        StreamableLongScatterSet(StreamInput in) throws IOException {
            super();
            readFrom(in);
        }

        StreamableLongScatterSet(int expectedElements) {
            super(expectedElements);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            keys = in.readVLongArray();
            assigned = in.readVInt();
            mask = in.readVInt();
            keyMixer = in.readVInt();
            resizeAt = in.readVInt();
            hasEmptyKey = in.readBoolean();
            loadFactor = in.readDouble();
            orderMixer = HashOrderMixing.none();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLongArray(this.keys);
            out.writeVInt(assigned);
            out.writeVInt(mask);
            out.writeVInt(keyMixer);
            out.writeVInt(resizeAt);
            out.writeBoolean(hasEmptyKey);
            out.writeDouble(loadFactor);
        }
    }

    private StreamableLongScatterSet longset;
    private BitSet bitset;
    private LongArrayList ranges;
    private int countOfBits = -1;
    private int countOfRanges = -1;
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

        if (range >= Integer.MIN_VALUE && range < Integer.MAX_VALUE - 1) {
            // the range of longs can scale to fit within 32 bits
            // so we're free to use a bitset
            this.bitset = new BitSet((int) range + 1);
            this.longset = null;
            this.min = min;
        } else {
            // we have to do something else
            this.bitset = null;
            this.longset = new StreamableLongScatterSet(32768);
            this.min = 0;
        }
    }

    public long[] getRanges() {
        return ranges == null ? null : ranges.buffer;
    }

    public void setAll(long[] bits, int length) {
        if (bitset != null) {
            for (int i = 0; i < length; i++)
                bitset.set((int) (bits[i] - min));

            for (LongIterator itr = iterator(true); itr.hasNext();) {
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
            for (int i=0; i<length; i++)
                longset.add(bits[i]);
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
            return longset.contains(value);
        }
    }

    public int getCountOfBits() {
        if (countOfBits == -1) {
            countOfBits = bitset != null ? bitset.cardinality() : 0;
        }

        return countOfBits;
    }

    public int getCountOfRanges() {
        if (countOfRanges == -1) {
            countOfRanges = 0;
            if (ranges != null) {
                for (int i=0; i<ranges.size(); i+=2) {
                    long low = ranges.get(i);
                    long high = ranges.get(i+1);
                    countOfRanges += (high-low);
                }
            }
        }
        return countOfRanges;
    }

    public int getCountOfLongs() {
        return longset.size();
    }

    public int getValueCount() {
        if (bitset != null) {
            return getCountOfBits() + getCountOfRanges();
        } else {
            return getCountOfLongs();
        }
    }

    public LongIterator iterator() {
        return iterator(true);
    }

    public LongIterator iterator(boolean bitset_only) {
        if (bitset != null) {
            int cnt = 1 + (bitset_only || ranges == null ? 0 : ranges.size()/2);
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

            if (!bitset_only && ranges != null) {
                for (int i = 0; i < ranges.size(); i += 2) {
                    long low = ranges.get(i);
                    long high = ranges.get(i + 1);

                    iterators[idx++] = LongIterator.create(LongStream.rangeClosed(low, high).iterator());
                }
            }

            return LongIterator.create(iterators);

        } else if (longset.size() > 0) {
            long[] values = new long[longset.size()];

            int i=0;
            for (LongCursor longCursor : longset) {
                values[i++] = longCursor.value;
            }
            Arrays.sort(values);
            return new LongArrayMergeSortIterator(new long[][] { values }, new int[] { values.length});
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
            countOfBits = in.readVInt();
            countOfRanges = in.readVInt();
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
            longset = new StreamableLongScatterSet(in);
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
            out.writeVInt(countOfRanges);
            NoCopyByteArrayOutputStream baos = new NoCopyByteArrayOutputStream(4096);
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(bitset);
                oos.flush();
            }
            out.writeVInt(baos.getNumBytes());
            out.writeBytes(baos.getBytes(), 0, baos.getNumBytes());
        } else {
            longset.writeTo(out);
        }

        out.writeBoolean(ranges != null);
        if (ranges != null) {
            DeltaEncoder.encode_longs_as_deltas(ranges.buffer, ranges.elementsCount, out);
        }
    }

    @Override
    public int hashCode() {
        int hash = 1;

        return 31 * hash + Objects.hash(longset, longset, bitset, ranges, countOfBits, countOfRanges, min);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        NumberArrayLookup other = (NumberArrayLookup) obj;
        return Objects.equals(longset, other.longset) &&
                Objects.equals(bitset, other.bitset) &&
                Objects.equals(ranges, other.ranges) &&
                Objects.equals(countOfBits, other.countOfBits) &&
                Objects.equals(countOfRanges, other.countOfRanges) &&
                Objects.equals(min, other.min);
    }
}
