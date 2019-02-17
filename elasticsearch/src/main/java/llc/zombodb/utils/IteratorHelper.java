package llc.zombodb.utils;

import java.util.PrimitiveIterator;

public interface IteratorHelper {

    static PrimitiveIterator.OfLong create(PrimitiveIterator.OfLong... iterators) {
        return new PrimitiveIterator.OfLong() {
            private int current;

            @Override
            public boolean hasNext() {
                if (current >= iterators.length)
                    return false;
                else if (iterators[current].hasNext())
                    return true;
                else {
                    current++;
                    if (current >= iterators.length)
                        return false;
                    return iterators[current].hasNext();
                }
            }

            @Override
            public long nextLong() {
                return iterators[current].nextLong();
            }
        };
    }
}
