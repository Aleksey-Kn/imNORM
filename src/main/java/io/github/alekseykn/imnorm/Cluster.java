package io.github.alekseykn.imnorm;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public final class Cluster<Record> {
    private boolean redacted = false;
    private final TreeMap<String, Record> data;

    Cluster(TreeMap<String, Record> map) {
        data = map;
    }

    Cluster(String id, Record record) {
        data = new TreeMap<>();
        data.put(id, record);
    }

    void set(String key, Record record) {
        redacted = true;
        data.put(key, record);
    }

    Record get(String key) {
        return data.get(key);
    }

    Collection<Record> findAll() {
        return data.values();
    }

    Record delete(String key) {
        redacted = true;
        return data.remove(key);
    }

    int size() {
        return data.size();
    }

    boolean containsKey(String key) {
        return data.containsKey(key);
    }

    Object firstKey() {
        return data.firstKey();
    }

    boolean isEmpty() {
        return size() == 0;
    }

    boolean isRedacted() {
        return redacted;
    }

    Cluster<Record> split() {
        TreeMap<String, Record> newClusterData = new TreeMap<>();
        int counter = 0;
        final int median = data.size() / 2;
        for(Map.Entry<String, Record> entry: data.entrySet()) {
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
