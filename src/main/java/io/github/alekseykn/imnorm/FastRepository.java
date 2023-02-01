package io.github.alekseykn.imnorm;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public final class FastRepository<Value> extends Repository<Value> {
    private final TreeMap<String, ConcurrentHashMap<Object, Value>> data = new TreeMap<>();

    FastRepository(Class<Value> type, File directory) {
        super(type, directory);
        Scanner scanner;
        Value now;
        ConcurrentHashMap<Object, Value> cluster;
        try {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                cluster = data.put(file.getName(), new ConcurrentHashMap<>());
                assert cluster != null;
                scanner = new Scanner(file);
                while (scanner.hasNextLine()) {
                    now = gson.fromJson(scanner.nextLine(), type);
                    cluster.put(recordId.get(now), now);
                }
            }
        } catch (FileNotFoundException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Value save(Value o) {
        return null;
    }

    @Override
    public Value find(Object id) {
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
    public Value delete(Object id) {
        return null;
    }
}
