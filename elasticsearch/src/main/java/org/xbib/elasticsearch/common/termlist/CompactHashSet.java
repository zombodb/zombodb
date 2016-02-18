package org.xbib.elasticsearch.common.termlist;

import java.util.*;

/**
 * CompactHashSet - implementation taken from
 * http://code.google.com/p/ontopia/source/browse/trunk/ontopia/src/java/net/ontopia/utils/CompactHashSet.java?r=1704
 * (License Apache 2.0)
 * load factor increased to 0.9
 */
public class CompactHashSet<E> extends AbstractSet<E>
        implements Set<E> {

    private final static int INITIAL_SIZE = 3;

    private final static double LOAD_FACTOR = 0.9;

    private final E nullObject = (E) new Object();

    private final E deletedObject = (E) new Object();

    private int elements;

    private int freecells;

    private E[] objects;

    private int modCount;

    public CompactHashSet() {
        objects = (E[]) new Object[INITIAL_SIZE];
        elements = 0;
        freecells = objects.length;
        modCount = 0;
    }

    public CompactHashSet(int size) {
        objects = (E[]) new Object[(size == 0 ? 1 : size)];
        elements = 0;
        freecells = objects.length;
        modCount = 0;
    }

    public Iterator<E> iterator() {
        return new CompactHashIterator();
    }

    public int size() {
        return elements;
    }

    @Override
    public boolean isEmpty() {
        return elements == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            o = nullObject;
        }
        int hash = o.hashCode();
        int index = (hash & 0x7FFFFFFF) % objects.length;
        int offset = 1;
        while (objects[index] != null
                && !(objects[index].hashCode() == hash
                && objects[index].equals(o))) {
            index = ((index + offset) & 0x7FFFFFFF) % objects.length;
            offset = offset * 2 + 1;

            if (offset == -1) {
                offset = 2;
            }
        }
        return objects[index] != null;
    }

    @Override
    public boolean add(E e) {
        if (e == null) {
            e = nullObject;
        }
        int hash = e.hashCode();
        int index = (hash & 0x7FFFFFFF) % objects.length;
        int offset = 1;
        int deletedix = -1;
        while (objects[index] != null
                && !(objects[index].hashCode() == hash
                && objects[index].equals(e))) {
            if (objects[index] == deletedObject) {
                deletedix = index;
            }
            index = ((index + offset) & 0x7FFFFFFF) % objects.length;
            offset = offset * 2 + 1;
            if (offset == -1) {
                offset = 2;
            }
        }
        if (objects[index] == null) {
            if (deletedix != -1) {
                index = deletedix;
            } else {
                freecells--;
            }
            modCount++;
            elements++;
            objects[index] = e;
            if (1 - (freecells / (double) objects.length) > LOAD_FACTOR) {
                rehash(objects.length);
                if (1 - (freecells / (double) objects.length) > LOAD_FACTOR) {
                    rehash(objects.length * 2 + 1);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            o = nullObject;
        }
        int hash = o.hashCode();
        int index = (hash & 0x7FFFFFFF) % objects.length;
        int offset = 1;
        while (objects[index] != null
                && !(objects[index].hashCode() == hash
                && objects[index].equals(o))) {
            index = ((index + offset) & 0x7FFFFFFF) % objects.length;
            offset = offset * 2 + 1;
            if (offset == -1) {
                offset = 2;
            }
        }
        if (objects[index] != null) {
            objects[index] = deletedObject;
            modCount++;
            elements--;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void clear() {
        elements = 0;
        for (int ix = 0; ix < objects.length; ix++) {
            objects[ix] = null;
        }
        freecells = objects.length;
        modCount++;
    }

    @Override
    public Object[] toArray() {
        Object[] result = new Object[elements];
        Object[] obj = this.objects;
        int pos = 0;
        for (int i = 0; i < obj.length; i++) {
            if (obj[i] != null && obj[i] != deletedObject) {
                if (obj[i] == nullObject) {
                    result[pos++] = null;
                } else {
                    result[pos++] = obj[i];
                }
            }
        }
        return result;
    }

    @Override
    public Object[] toArray(Object a[]) {
        int size = elements;
        if (a.length < size) {
            a = (Object[]) java.lang.reflect.Array.newInstance(
                    a.getClass().getComponentType(), size);
        }
        Object[] obj = this.objects;
        int pos = 0;
        for (int i = 0; i < obj.length; i++) {
            if (obj[i] != null && obj[i] != deletedObject) {
                if (obj[i] == nullObject) {
                    a[pos++] = null;
                } else {
                    a[pos++] = obj[i];
                }
            }
        }
        return a;
    }

    protected void rehash(int newCapacity) {
        Object[] newObjects = new Object[newCapacity];
        for (Object o : objects) {
            if (o == null || o == deletedObject) {
                continue;
            }
            int hash = o.hashCode();
            int index = (hash & 0x7FFFFFFF) % newCapacity;
            int offset = 1;
            while (newObjects[index] != null) {
                index = ((index + offset) & 0x7FFFFFFF) % newCapacity;
                offset = offset * 2 + 1;

                if (offset == -1) {
                    offset = 2;
                }
            }

            newObjects[index] = o;
        }
        objects = (E[]) newObjects;
        freecells = objects.length - elements;
    }

    private class CompactHashIterator implements Iterator<E> {

        private int index;
        private int lastReturned = -1;
        private int expectedModCount;

        public CompactHashIterator() {
            for (index = 0; index < objects.length
                    && (objects[index] == null
                    || objects[index] == deletedObject); index++) {
                ;
            }
            expectedModCount = modCount;
        }

        public boolean hasNext() {
            return index < objects.length;
        }

        public E next() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            int length = objects.length;
            if (index >= length) {
                lastReturned = -2;
                throw new NoSuchElementException();
            }

            lastReturned = index;
            for (index += 1; index < length
                    && (objects[index] == null
                    || objects[index] == deletedObject); index++) {
                ;
            }
            if (objects[lastReturned] == nullObject) {
                return null;
            } else {
                return objects[lastReturned];
            }
        }

        public void remove() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            if (lastReturned == -1 || lastReturned == -2) {
                throw new IllegalStateException();
            }
            if (objects[lastReturned] != null && objects[lastReturned] != deletedObject) {
                objects[lastReturned] = deletedObject;
                elements--;
                modCount++;
                expectedModCount = modCount;
            }
        }
    }
}