package io.github.alekseykn.imnorm;

import java.io.File;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public final class FastRepository<Value, Key> extends Repository<Value, Key> {
    private final TreeMap<String, ConcurrentHashMap<Key, Value>> data = new TreeMap<>();

    FastRepository(Class<Value> type, File directory) {
        super(type, directory);
    }

    @Override
    public Value save(Value o) {
        return null;
    }

    @Override
    public Value find(Key id) {
        return null;
    }

    @Override
    public Set<Value> findAll() {
        return null;
    }

    @Override
    public Set<Value> findAll(int startIndex, int rowCount) {
        return null;
    }

    @Override
    public Value delete(Key id) {
        return null;
    }
}
