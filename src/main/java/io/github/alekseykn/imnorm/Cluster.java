package io.github.alekseykn.imnorm;

import java.util.Collection;
import java.util.HashMap;

public final class Cluster<Value> {
    private boolean redacted = false;
    private final HashMap<Object, Value> data;

    Cluster(HashMap<Object, Value> map) {
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

    boolean isRedacted() {
        return redacted;
    }

    void wasFlush() {
        redacted = false;
    }
}
