package io.github.alekseykn.imnorm;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Cluster<Record> {
    private boolean redacted = false;
    private final ConcurrentHashMap<Object, Record> data;

    Cluster(ConcurrentHashMap<Object, Record> map) {
        data = map;
    }

    Cluster(Object id, Record record) {
        data = new ConcurrentHashMap<>();
        data.put(id, record);
    }

    void set(Object key, Record record) {
        redacted = true;
        data.put(key, record);
    }

    Record get(Object key) {
        return data.get(key);
    }

    Collection<Record> findAll() {
        return data.values();
    }

    Record delete(Object key) {
        redacted = true;
        return data.remove(key);
    }

    int size() {
        return data.size();
    }

    boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    Object firstKey() {
        return data.keySet().stream().min(Comparator.comparing(String::valueOf));
    }

    boolean isEmpty() {
        return size() == 0;
    }

    boolean isRedacted() {
        return redacted;
    }

    Cluster<Record> split() {
        ConcurrentHashMap<Object, Record> newClusterData = new ConcurrentHashMap<>();
        int counter = 0;
        final int median = data.size() / 2;
        for(Map.Entry<Object, Record> entry: data.entrySet()) {
            if(counter++ > median) {
                newClusterData.put(entry.getKey(), entry.getValue());
                data.remove(entry.getKey());
            }
        }
        return new Cluster<>(newClusterData);
    }

    void wasFlush() {
        redacted = false;
    }
}
