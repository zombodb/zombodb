package llc.zombodb.utils;

public class NumberArrayLookupMergeSortIterator implements LongIterator {
    // Thanks, @ShitalShah from https://stackoverflow.com/a/31310853 for the inspiration

    private final LongIterator[] iterators;
    private final int[] lengths;
    private final int finalTotal;
    private int total;

    private long pushback;
    private boolean havePushback = false;

    public NumberArrayLookupMergeSortIterator(NumberArrayLookup[] arrays) {
        this.iterators = new LongIterator[arrays.length];
        this.lengths = new int[arrays.length];
        for (int i=0; i<arrays.length; i++) {
            NumberArrayLookup lookup = arrays[i];

            iterators[i] = lookup.iterator();
            lengths[i] = lookup.getValueCount();
            total += lengths[i];
        }
        this.finalTotal = total;
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
                if (next < value) {
                    if (winner != null) // push the old value back
                        winner.push(value);
                    value = next;
                    winner = itr;
                } else {
                    itr.push(next);
                }
            }
        }
        return value;
    }

    public boolean hasNext() {
        return total > 0 || havePushback;
    }

    public int getTotal() {
        return finalTotal;
    }
}
