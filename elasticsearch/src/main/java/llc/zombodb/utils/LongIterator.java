package llc.zombodb.utils;

import java.util.PrimitiveIterator;

public interface LongIterator {


    static LongIterator create(PrimitiveIterator.OfLong iterator) {
        return new LongIterator() {
            private long pushback;
            private boolean havePushback;

            @Override
            public boolean hasNext() {
                return havePushback || iterator.hasNext();
            }

            @Override
            public long next() {
                if (havePushback) {
                    havePushback = false;
                    return pushback;
                }
                return iterator.nextLong();
            }

            @Override
            public void push(long value) {
                pushback = value;
                havePushback = true;
            }
        };
    }

    static LongIterator create(LongIterator... iterators) {
        if (iterators.length == 1)
            return iterators[0];

        return new LongIterator() {
            private long pushback;
            private boolean havePushback;

            @Override
            public boolean hasNext() {
                if (havePushback)
                    return true;

                for (LongIterator itr : iterators)
                    if (itr.hasNext())
                        return true;
                return false;
            }

            @Override
            public long next() {
                if (havePushback) {
                    havePushback = false;
                    return pushback;
                }

                long value = Long.MAX_VALUE;
                LongIterator winner = null;

                for (LongIterator itr : iterators) {
                    if (itr.hasNext()) {
                        long next = itr.next();
                        if (next <= value) {
                            if (winner != null) // push the old value back
                                winner.push(value);
                            value = next;
                            winner = itr;
                        } else {
                            itr.push(next);
                        }
                    }
                }

                assert(winner != null);

                return value;
            }

            @Override
            public void push(long value) {
                pushback = value;
                havePushback = true;
            }
        };
    }

    boolean hasNext();

    long next();

    void push(long value);
}
