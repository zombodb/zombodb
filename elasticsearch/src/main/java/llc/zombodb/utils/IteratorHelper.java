package llc.zombodb.utils;

import java.util.PrimitiveIterator;

public interface IteratorHelper {

    static PrimitiveIterator.OfLong create(PrimitiveIterator.OfLong... iterators) {
        return new PrimitiveIterator.OfLong() {
            private int current;

            @Override
            public boolean hasNext() {
                while (current < iterators.length) {
                    if (iterators[current].hasNext())
                        return true;
                    current++;
                }
                return false;
            }

            @Override
            public long nextLong() {
                return iterators[current].nextLong();
            }
        };
    }
}
