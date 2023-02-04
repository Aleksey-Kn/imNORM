package io.github.alekseykn.imnorm;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Cluster<Record> {
    private boolean redacted = true;
    private final ConcurrentHashMap<String, Record> data;

    Cluster(ConcurrentHashMap<String, Record> map) {
        data = map;
    }

    Cluster(String id, Record record) {
        data = new ConcurrentHashMap<>();
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

    String firstKey() {
        return data.keySet().stream().min(Comparator.comparing(s -> s)).orElseThrow();
    }

    boolean isEmpty() {
        return size() == 0;
    }

    boolean isRedacted() {
        return redacted;
    }

    Cluster<Record> split() {
        ConcurrentHashMap<String, Record> newClusterData = new ConcurrentHashMap<>();
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
