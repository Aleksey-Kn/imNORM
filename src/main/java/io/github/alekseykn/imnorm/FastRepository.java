package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    protected synchronized Value create(Object id, Value value) {
        String stringId = String.valueOf(id);
        if(needGenerateId) {
            //TODO: autogenerate
        }
        if (data.isEmpty() || data.firstKey().compareTo(stringId) < 0) {
            data.put(stringId, new Cluster<>(id, value));
        } else {
            Cluster<Value> currentCluster = findCurrentCluster(id);
            currentCluster.set(id, value);
            if(currentCluster.size() * sizeOfEntity > 100_000) {
                //TODO: add split cluster
            }
        }
        return value;
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
                        .sorted(new IdComparator(recordId))
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
                            PrintWriter printWriter = new PrintWriter(directory.getAbsolutePath() + entry.getKey());
                            entry.getValue().wasFlush();
                            entry.getValue().findAll().forEach(value -> printWriter.println(gson.toJson(value)));
                            printWriter.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                });
    }
}
