package llc.zombodb.utils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.PrimitiveIterator;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class IntOrLongBitmap implements java.io.Externalizable {
    /**
     * IMPORTANT:  we must have signed longs here, so the ctor arg must be 'true'
     * Otherwise we won't maintain sorting the way we need
     */
    private final Roaring64NavigableMap longs = new Roaring64NavigableMap(true);
    private final RoaringBitmap ints = new RoaringBitmap();

    public void add(long value) {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE)
            ints.add((int) value);
        else
            longs.addLong(value);
    }

    public boolean contains(long value) {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE)
            return ints.contains((int) value);
        else
            return longs.contains(value);
    }

    public int size() {
        return ints.getCardinality() + longs.getIntCardinality();
    }

    public PrimitiveIterator.OfLong iterator() {
        return IteratorHelper.create(
                new PrimitiveIterator.OfLong() {
                    final PeekableIntIterator itr = ints.getIntIterator();

                    @Override
                    public boolean hasNext() {
                        return itr.hasNext();
                    }

                    @Override
                    public long nextLong() {
                        return itr.next();
                    }
                },
                new PrimitiveIterator.OfLong() {
                    final LongIterator itr = longs.getLongIterator();

                    @Override
                    public boolean hasNext() {
                        return itr.hasNext();
                    }

                    @Override
                    public long nextLong() {
                        return itr.next();
                    }

                }
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(ints, longs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        IntOrLongBitmap other = (IntOrLongBitmap) obj;
        return Objects.equals(this.ints, other.ints) &&
                Objects.equals(this.longs, other.longs);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ints.writeExternal(out);
        longs.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ints.readExternal(in);
        longs.readExternal(in);
    }
}
