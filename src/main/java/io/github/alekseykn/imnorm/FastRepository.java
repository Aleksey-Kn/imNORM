package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public final class FastRepository<Value> extends Repository<Value> {
    private final TreeMap<String, Cluster<Value>> data = new TreeMap<>();

    FastRepository(Class<Value> type, File directory) {
        super(type, directory);
        Scanner scanner;
        Value now;
        HashMap<Object, Value> tempClusterData;
        try {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                tempClusterData = new HashMap<>();
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
        return data.values().stream().map(Cluster::findAll).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public Set<Value> findAll(int startIndex, int rowCount) {
        LinkedHashSet<Value> result = new LinkedHashSet<>(rowCount);
        List<Value> afterSkippedClusterValues;
        for (Collection<Value> clusterRecord
                : data.values().stream().map(Cluster::findAll).collect(Collectors.toList())) {
            if (clusterRecord.size() < startIndex) {
                startIndex -= clusterRecord.size();
            } else {
                afterSkippedClusterValues = clusterRecord.stream()
                        .sorted((first, second) -> {
                            try {
                                Object firstKey = recordId.get(first);
                                Object secondKey = recordId.get(second);
                                if (firstKey instanceof Comparable) {
                                    return ((Comparable) firstKey).compareTo(secondKey);
                                } else {
                                    return String.valueOf(firstKey).compareTo(String.valueOf(secondKey));
                                }
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .skip(startIndex)
                        .limit(rowCount)
                        .collect(Collectors.toList());
                result.addAll(afterSkippedClusterValues);

                rowCount -= afterSkippedClusterValues.size();
                startIndex = 0;
            }
            if (rowCount == 0)
                break;
        }
        return result;
    }

    @Override
    public void flush() {
        data.entrySet().parallelStream()
                .filter(e -> e.getValue().isRedacted())
                .forEach(entry -> {
                    try {
                        synchronized (entry.getValue()) {
                            PrintWriter printWriter = new PrintWriter(directory.getAbsolutePath() + entry.getKey());
                            entry.getValue().findAll().forEach(value -> printWriter.println(gson.toJson(value)));
                            printWriter.close();
                            entry.getValue().wasFlush();
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                });
    }
}
