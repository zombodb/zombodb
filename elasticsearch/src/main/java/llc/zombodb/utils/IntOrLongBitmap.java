package llc.zombodb.utils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.PrimitiveIterator;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class IntOrLongBitmap implements java.io.Externalizable {
    private Roaring64NavigableMap longs;
    private RoaringBitmap ints = new RoaringBitmap();

    public void add(long value) {
        if (longs == null && value >= 0 && value <= Integer.MAX_VALUE) {
            // it fits within the space of a positive Integer (and we're still using ints)
            // so just carry on
            ints.add((int) value);
        } else {
            // we've either flipped to using longs already
            // or we need to do that now
            try {
                longs.addLong(value);
            } catch (NullPointerException npe) {
                // IMPORTANT:  we must have signed longs here, so the ctor arg must be 'true'
                // Otherwise we won't maintain sorting the way we need
                longs = new Roaring64NavigableMap(true);

                // copy any ints we might already have into the longs
                for (IntIterator itr = ints.getIntIterator(); itr.hasNext(); )
                    longs.addLong(itr.next());

                // add the value the user wanted to add in the first place
                longs.addLong(value);

                // finally, we don't need/want "ints" anymore
                ints = null;
            }
        }
    }

    public boolean contains(long value) {
        return longs != null ? longs.contains(value) : ints.contains((int) value);
    }

    public int size() {
        return longs != null ? longs.getIntCardinality() : ints.getCardinality();
    }

    public PrimitiveIterator.OfLong iterator() {
        return longs != null ?
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
                } :
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
                };
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
        out.writeBoolean(ints != null);
        out.writeBoolean(longs != null);

        if (ints != null)
            ints.writeExternal(out);
        if (longs != null)
            longs.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean haveInts = in.readBoolean();
        boolean haveLongs = in.readBoolean();

        if (haveInts)
            ints.readExternal(in);
        if (haveLongs) {
            longs = new Roaring64NavigableMap(true);
            longs.readExternal(in);
        }
    }
}
