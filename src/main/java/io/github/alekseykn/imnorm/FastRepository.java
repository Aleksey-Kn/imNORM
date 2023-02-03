package io.github.alekseykn.imnorm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public final class FastRepository<Record> extends Repository<Record> {
    private final TreeMap<String, Cluster<Record>> data = new TreeMap<>();

    FastRepository(Class<Record> type, File directory) {
        super(type, directory);
        Scanner scanner;
        Record now;
        TreeMap<String, Record> tempClusterData;
        try {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                tempClusterData = new TreeMap<>();
                scanner = new Scanner(file);
                while (scanner.hasNextLine()) {
                    now = gson.fromJson(scanner.nextLine(), type);
                    tempClusterData.put(getStringIdFromRecord.apply(now), now);
                }
                data.put(file.getName(), new Cluster<>(tempClusterData));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected synchronized Cluster<Record> findCurrentCluster(Object id) {
        return data.floorEntry(String.valueOf(id)).getValue();
    }

    @Override
    protected synchronized Record create(Object id, Record record) {
        String stringId = String.valueOf(id);
        if(needGenerateId) {
            //TODO: autogenerate
        }
        if (data.isEmpty() || data.firstKey().compareTo(stringId) < 0) {
            data.put(stringId, new Cluster<>(stringId, record));
        } else {
            Cluster<Record> currentCluster = findCurrentCluster(id);
            currentCluster.set(stringId, record);
            if(currentCluster.size() * sizeOfEntity > 100_000) {
                Cluster<Record> newCluster = currentCluster.split();
                data.put(String.valueOf(currentCluster.firstKey()), newCluster);
            }
        }
        return record;
    }

    @Override
    public synchronized Set<Record> findAll() {
        return data.values().stream().map(Cluster::findAll).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public synchronized Set<Record> findAll(int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        for (Collection<Record> clusterRecord
                : data.values().stream().map(Cluster::findAll).collect(Collectors.toList())) {
            if (clusterRecord.size() < startIndex) {
                startIndex -= clusterRecord.size();
            } else {
                afterSkippedClusterValues = clusterRecord.stream()
                        .sorted(Comparator.comparing(getStringIdFromRecord))
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
    public synchronized void flush() {
        data.entrySet().parallelStream()
                .filter(e -> e.getValue().isRedacted())
                .forEach(entry -> {
                    try {
                        PrintWriter printWriter = new PrintWriter(directory.getAbsolutePath() + entry.getKey());
                        entry.getValue().findAll().forEach(record -> printWriter.println(gson.toJson(record)));
                        entry.getValue().wasFlush();
                        printWriter.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                });
    }
}

