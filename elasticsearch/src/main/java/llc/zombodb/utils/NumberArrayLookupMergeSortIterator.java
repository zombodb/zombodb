package llc.zombodb.utils;

public class NumberArrayLookupMergeSortIterator implements LongIterator {

    private final LongIterator wrapped;
    private int total;

    public NumberArrayLookupMergeSortIterator(NumberArrayLookup[] lookups) {
        LongIterator[] iterators = new LongIterator[lookups.length];
        for (int i=0; i<lookups.length; i++) {
            iterators[i] = lookups[i].iterator();
            total += lookups[i].getValueCount();
        }

        wrapped = LongIterator.create(iterators);
    }

    @Override
    public boolean hasNext() {
        return wrapped.hasNext();
    }

    @Override
    public long next() {
        return wrapped.next();
    }

    @Override
    public void push(long value) {
        wrapped.push(value);
    }

    public int getTotal() {
        return total;
    }
}
