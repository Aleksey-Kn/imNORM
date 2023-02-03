package io.github.alekseykn.imnorm;

import java.util.Collection;
import java.util.ConcurrentHashMap;

public final class Cluster<Value> {
    private boolean redacted = false;
    private final ConcurrentHashMap<Object, Value> data;

    Cluster(ConcurrentHashMap<Object, Value> map) {
        data = map;
    }

    void set(Object key, Value value) {
        redacted = true;
        data.put(key, value);
    }

    Value get(Object key) {
        return data.get(key);
    }

    Collection<Value> findAll() {
        return data.values();
    }

    Value delete(Object key) {
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
        return data.keySet().stream()
                .min((first, second) -> {
                    if (first instanceof Comparable) {
                        return ((Comparable) first).compareTo(second);
                    } else {
                        return String.valueOf(first).compareTo(String.valueOf(second));
                    }
                })
                .orElseThrow();
    }
    
    boolean isEmpty() {
        return size() == 0;
    }

    boolean isRedacted() {
        return redacted;
    }

    void wasFlush() {
        redacted = false;
    }
}
