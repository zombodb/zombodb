package llc.zombodb.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.PrimitiveIterator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

/**
 * Allows quick lookup for a value in an array of longs.
 * <p>
 * If the array of values can fit within 32bits, we scale each value so we
 * can use a bitset for O(1) lookup,
 * <p>
 * Otherwise the array of values is sorted and we binary search for O(log(n)) lookup.
 */
public class NumberArrayLookup implements Streamable {

    private IntOrLongBitmap bitmap;

    public static NumberArrayLookup fromStreamInput(StreamInput in) throws IOException {
        NumberArrayLookup nal = new NumberArrayLookup();
        nal.readFrom(in);
        return nal;
    }

    private NumberArrayLookup() {
        // noop
    }

    public NumberArrayLookup(IntOrLongBitmap bitmap) {
        this.bitmap = bitmap;
    }

    /**
     * Is the specific value contained within?
     */
    public boolean get(long value) {
        return bitmap.contains(value);
    }

    public int size() {
        return bitmap.size();
    }

    public PrimitiveIterator.OfLong[] iterators() {
        return bitmap.iterators();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        try {
            ObjectInputStream ois = new ObjectInputStream(in);
            bitmap = new IntOrLongBitmap(ois);
        } catch (ClassNotFoundException cnfe) {
            throw new IOException(cnfe);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(out);
        bitmap.writeExternal(oos);
        oos.flush();
    }

    @Override
    public int hashCode() {
        return bitmap.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass())
            return false;

        NumberArrayLookup other = (NumberArrayLookup) obj;
        return bitmap.equals(other.bitmap);
    }
}
