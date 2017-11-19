package llc.zombodb.utils;

public interface LongIterator {

    boolean hasNext();

    long next();

    void push(long value);
}
