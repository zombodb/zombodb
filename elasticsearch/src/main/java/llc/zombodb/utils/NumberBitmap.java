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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class NumberBitmap implements Streamable {
    private Roaring64NavigableMap negatives;
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

    public NumberBitmap(StreamInput ois) throws IOException {
        readFrom(ois);
    }

    public NumberBitmap() {
        negatives = make64bitBitmap();
        ints = mak32bitBitmap();
        scaledints = mak32bitBitmap();
        longs = make64bitBitmap();
    }

    public void add(long value) {

        if (value < 0) {
            // negative numbers get stored as longs
            negatives.addLong(value);
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
            return negatives.contains(value);
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

        size += negatives.getIntCardinality();
        size += ints.getCardinality();
        size += scaledints.getCardinality();
        size += longs.getIntCardinality();

        return size;
    }

    public PrimitiveIterator.OfLong iterator() {
        return IteratorHelper.create(negatives_iterator(), ints_iterator(), scaledints_iterator(), longs_iterator());
    }

    @Override
    public int hashCode() {
        return Objects.hash(negatives, ints, scaledints, longs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        NumberBitmap other = (NumberBitmap) obj;
        return Objects.equals(this.negatives, other.negatives) &&
                Objects.equals(this.ints, other.ints) &&
                Objects.equals(this.scaledints, other.scaledints) &&
                Objects.equals(this.longs, other.longs);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataOutput dataOutput = new DataOutputStream(out);
        negatives.runOptimize();
        negatives.serialize(dataOutput);

        ints.runOptimize();
        ints.serialize(dataOutput);

        scaledints.runOptimize();
        scaledints.serialize(dataOutput);

        longs.runOptimize();
        longs.serialize(dataOutput);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        DataInput dataInput = new DataInputStream(in);
        negatives = make64bitBitmap();
        negatives.deserialize(dataInput);

        ints = mak32bitBitmap();
        ints.deserialize(dataInput);

        scaledints = mak32bitBitmap();
        scaledints.deserialize(dataInput);

        longs = make64bitBitmap();
        longs.deserialize(dataInput);
    }

    private PrimitiveIterator.OfLong negatives_iterator() {
        return new PrimitiveIterator.OfLong() {
            final LongIterator itr = negatives.getLongIterator();

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
        return negatives.serializedSizeInBytes() + ints.serializedSizeInBytes() + scaledints.serializedSizeInBytes() + longs.serializedSizeInBytes();
    }

    public void merge(NumberBitmap bitmap) {
        negatives.or(bitmap.negatives);
        ints.or(bitmap.ints);
        scaledints.or(bitmap.scaledints);
        longs.or(bitmap.longs);
    }
}
