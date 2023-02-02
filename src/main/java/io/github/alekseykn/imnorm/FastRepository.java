package io.github.alekseykn.imnorm;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public final class FastRepository<Value> extends Repository<Value> {
    private final TreeMap<String, Cluster<Value>> data = new TreeMap<>();

    FastRepository(Class<Value> type, File directory) {
        super(type, directory);
        Scanner scanner;
        Value now;
        ConcurrentHashMap<Object, Value> tempClusterData;
        try {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                tempClusterData = new ConcurrentHashMap<>();
                scanner = new Scanner(file);
                while (scanner.hasNextLine()) {
                    now = gson.fromJson(scanner.nextLine(), type);
                    tempClusterData.put(recordId.get(now), now);
                }
                data.put(file.getName(), new Cluster<>(tempClusterData));
            }
        } catch (FileNotFoundException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Cluster<Value> findCurrentCluster(Object recordInCluster) {
        try {
            return data.floorEntry(String.valueOf(recordId.get(recordInCluster))).getValue();
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e.getMessage());
        }
    }

    @Override
    protected Value create(Object id, Value value) {
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
    public void flush() {
        data.entrySet().parallelStream()
                .filter(e -> e.getValue().isRedacted())
                .forEach(entry -> {
                    try {
                        PrintWriter printWriter = new PrintWriter(directory.getAbsolutePath() + entry.getKey());
                        entry.getValue().findAll().forEach(value -> printWriter.println(gson.toJson(value)));
                        printWriter.close();
                        entry.getValue().wasFlush();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                });
    }
}
