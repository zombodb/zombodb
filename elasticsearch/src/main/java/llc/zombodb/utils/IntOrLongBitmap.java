package llc.zombodb.utils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.PrimitiveIterator;

import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class IntOrLongBitmap implements java.io.Externalizable {
    private BitSet ints;
    private BitSet scaledints;
    private Roaring64NavigableMap longs;

    private static Roaring64NavigableMap make_longs() {
        // IMPORTANT:  we must have signed longs here, so the ctor arg must be 'true'
        // Otherwise we won't maintain sorting the way we need
        return new Roaring64NavigableMap(true);
    }

    public IntOrLongBitmap(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        readExternal(ois);
    }

    public IntOrLongBitmap() {
        ints = new BitSet();
        scaledints = new BitSet();
        longs = make_longs();
    }

    public void add(long value) {

        if (value < 0) {
            // negative numbers get stored as longs
            longs.addLong(value);
        } else if (value < Integer.MAX_VALUE) {
            // positive ints get stored as ints
            ints.set((int) value);
        } else {
            long diff = value - Integer.MAX_VALUE;

            if (diff < Integer.MAX_VALUE) {
                // we can scale it
                scaledints.set((int) diff);
            } else {
                // it's a long
                longs.addLong(value);
            }
        }
    }

    public boolean contains(long value) {
        if (value < 0) {
            return longs.contains(value);
        } else {
            if (value < Integer.MAX_VALUE) {
                return ints.get((int) value);
            } else {
                long diff = value - Integer.MAX_VALUE;

                if (diff < Integer.MAX_VALUE) {
                    return scaledints.get((int) diff);
                } else {
                    return longs.contains(value);
                }
            }
        }
    }

    public int size() {
        int size = 0;

        size += ints.cardinality();
        size += scaledints.cardinality();
        size += longs.getIntCardinality();

        return size;
    }

    PrimitiveIterator.OfLong[] iterators() {
        List<PrimitiveIterator.OfLong> iterators = Arrays.asList(ints_iterator(), scaledints_iterator(), longs_iterator());
        return iterators.toArray(new PrimitiveIterator.OfLong[0]);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ints, scaledints, longs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        IntOrLongBitmap other = (IntOrLongBitmap) obj;
        return Objects.equals(this.ints, other.ints) &&
                Objects.equals(this.scaledints, other.scaledints) &&
                Objects.equals(this.longs, other.longs);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ints);
        out.writeObject(scaledints);
        longs.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ints = (BitSet) in.readObject();
        scaledints = (BitSet) in.readObject();

        longs = make_longs();
        longs.readExternal(in);
    }

    private PrimitiveIterator.OfLong ints_iterator() {
        return new PrimitiveIterator.OfLong() {
            private int value = ints.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return value >= 0;
            }

            @Override
            public long nextLong() {
                long value = this.value;
                this.value = ints.nextSetBit(this.value + 1);
                return value;
            }
        };
    }

    private PrimitiveIterator.OfLong scaledints_iterator() {
        return new PrimitiveIterator.OfLong() {
            private int value = scaledints.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return value >= 0;
            }

            @Override
            public long nextLong() {
                long value = this.value;
                this.value = scaledints.nextSetBit(this.value + 1);
                return value + Integer.MAX_VALUE; // make sure to scale the value
            }
        };
    }

    private PrimitiveIterator.OfLong longs_iterator() {
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
}
