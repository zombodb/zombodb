package llc.zombodb.utils;

import java.util.Iterator;

public class PushbackIterator<T> implements Iterator<T> {
    private Iterator<T> itr;
    private T pushback;


    public PushbackIterator(Iterator<T> itr) {
        this.itr = itr;
    }

    public void push(T value) {
        pushback = value;
    }

    @Override
    public boolean hasNext() {
        return pushback != null || itr.hasNext();
    }

    @Override
    public T next() {
        T value = pushback != null ? pushback : itr.next();
        pushback = null;
        return value;
    }

    @Override
    public void remove() {
        itr.remove();
    }
}