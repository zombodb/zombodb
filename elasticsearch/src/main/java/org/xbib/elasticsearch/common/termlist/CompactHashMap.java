package org.xbib.elasticsearch.common.termlist;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * A wrapper around jdbm2's PrimaryTreeMap that manages its own
 * on-disk file
 */
public class CompactHashMap<K extends Comparable & Serializable, V extends Serializable> implements Map<K, V> {
    private final File path;
    private final RecordManager recman;
    private final PrimaryTreeMap<K, V> map;

    public CompactHashMap() {
        try {
            path = File.createTempFile("CompactHashMap", ".jdbm");
            recman = RecordManagerFactory.createRecordManager(path.getAbsolutePath());
            map = recman.treeMap("CompactHashMap");
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public File getPath() {
        return path;
    }

    public void commit() throws IOException {
        recman.commit();
    }


    //
    // java.util.Map implementation follows
    //

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        recman.close();
        System.err.println(path);
    }
}
