package llc.zombodb.utils;

public class NumberArrayLookupMergeSortIterator implements LongIterator {
    // Thanks, @ShitalShah from https://stackoverflow.com/a/31310853 for the inspiration

    private final LongIterator[] iterators;
    private final int finalTotal;
    private int total;

    private long pushback;
    private boolean havePushback = false;

    public NumberArrayLookupMergeSortIterator(NumberArrayLookup[] arrays) {
        if (arrays != null) {
            this.iterators = new LongIterator[arrays.length];
            for (int i = 0; i < arrays.length; i++) {
                NumberArrayLookup lookup = arrays[i];

                iterators[i] = lookup.iterator();
                total += lookup.getValueCount();
            }
            this.finalTotal = total;
        } else {
            this.iterators = new LongIterator[0];
            this.finalTotal = 0;
        }
    }

    public void push(long value) {
        pushback = value;
        havePushback = true;
    }

    public long next() {
        if (havePushback) {
            havePushback = false;
            return pushback;
        }

        --total;

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

    public boolean hasNext() {
        return total > 0 || havePushback;
    }

    public int getTotal() {
        return finalTotal;
    }
}
