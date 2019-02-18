package llc.zombodb.utils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.PrimitiveIterator;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class IntOrLongBitmap implements java.io.Externalizable {
    private RoaringBitmap ints;
    private RoaringBitmap scaledints;
    private Roaring64NavigableMap longs;

    private static Roaring64NavigableMap make64bitBitmap() {
        // IMPORTANT:  we must have signed longs here, so the ctor arg must be 'true'
        // Otherwise we won't maintain sorting the way we need
        return new Roaring64NavigableMap(true);
    }

    private static RoaringBitmap mak32bitBitmap() {
        return new RoaringBitmap();
    }

    public IntOrLongBitmap(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        readExternal(ois);
    }

    public IntOrLongBitmap() {
        ints = mak32bitBitmap();
        scaledints = mak32bitBitmap();
        longs = make64bitBitmap();
    }

    public void add(long value) {

        if (value < 0) {
            // negative numbers get stored as longs
            longs.addLong(value);
        } else if (value < Integer.MAX_VALUE) {
            // positive ints get stored as ints
            ints.add((int) value);
        } else {
            long diff = value - Integer.MAX_VALUE;

            if (diff < Integer.MAX_VALUE) {
                // we can scale it
                scaledints.add((int) diff);
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
                return ints.contains((int) value);
            } else {
                long diff = value - Integer.MAX_VALUE;

                if (diff < Integer.MAX_VALUE) {
                    return scaledints.contains((int) diff);
                } else {
                    return longs.contains(value);
                }
            }
        }
    }

    public int size() {
        int size = 0;

        size += ints.getCardinality();
        size += scaledints.getCardinality();
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
        ints.runOptimize();
        ints.serialize(out);

        scaledints.runOptimize();
        scaledints.serialize(out);

        longs.runOptimize();
        longs.serialize(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        ints = mak32bitBitmap();
        ints.deserialize(in);

        scaledints = mak32bitBitmap();
        scaledints.deserialize(in);

        longs = make64bitBitmap();
        longs.deserialize(in);
    }

    private PrimitiveIterator.OfLong ints_iterator() {
        return new PrimitiveIterator.OfLong() {
            private IntIterator itr = ints.getIntIterator();

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

    private PrimitiveIterator.OfLong scaledints_iterator() {
        return new PrimitiveIterator.OfLong() {
            private IntIterator itr = scaledints.getIntIterator();

            @Override
            public boolean hasNext() {
                return itr.hasNext();
            }

            @Override
            public long nextLong() {
                return itr.next() + Integer.MAX_VALUE;
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

    public long estimateByteSize() {
        return ints.serializedSizeInBytes() + scaledints.serializedSizeInBytes() + longs.serializedSizeInBytes();
    }
}
