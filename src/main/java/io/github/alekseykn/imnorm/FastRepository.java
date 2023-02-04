package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class FastRepository<Record> extends Repository<Record> {
    private final TreeMap<String, Cluster<Record>> data = new TreeMap<>();

    FastRepository(Class<Record> type, File directory) {
        super(type, directory);
        Scanner scanner;
        Record now;
        ConcurrentHashMap<String, Record> tempClusterData;
        try {
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                if(!file.getName().equals("_sequence.imnorm")) {
                    tempClusterData = new ConcurrentHashMap<>();
                    scanner = new Scanner(file);
                    while (scanner.hasNextLine()) {
                        now = gson.fromJson(scanner.nextLine(), type);
                        tempClusterData.put(getIdFromRecord.apply(now), now);
                    }
                    data.put(file.getName(), new Cluster<>(tempClusterData));
                }
            }
        } catch (FileNotFoundException e) {
            throw new InternalImnormException(e);
        }
    }

    @Override
    protected Cluster<Record> findCurrentClusterFromId(String id) {
        synchronized (data) {
            Map.Entry<String, Cluster<Record>> entry = data.floorEntry(id);
            if(Objects.isNull(entry)) {
                return null;
            } else {
                return entry.getValue();
            }
        }
    }

    @Override
    protected Record create(String id, Record record) {
        //blocking on 'data', because in the process of disbanding the cluster,
        //there will be no access to the newly created cluster from 'data'
        synchronized (data) {
            if (needGenerateId) {
                id = generateAndSetIdForRecord(record);
            }
            if (data.isEmpty() || data.firstKey().compareTo(id) > 0) {
                data.put(id, new Cluster<>(id, record));
            } else {
                Cluster<Record> currentCluster = findCurrentClusterFromId(id);
                assert currentCluster != null;
                currentCluster.set(id, record);
                if (currentCluster.size() * sizeOfEntity > 100_000) {
                    Cluster<Record> newCluster = currentCluster.split();
                    data.put(currentCluster.firstKey(), newCluster);
                }
            }
        }
        return record;
    }

    @Override
    public Set<Record> findAll() {
        waitAllRecord();
        synchronized (data) {
            return data.values().stream().map(Cluster::findAll).flatMap(Collection::stream).collect(Collectors.toSet());
        }
    }

    @Override
    public Set<Record> findAll(int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        List<Collection<Record>> clustersData;
        waitAllRecord();
        synchronized (data) {
            clustersData = data.values().stream().map(Cluster::findAll).collect(Collectors.toList());
        }
        for (Collection<Record> clusterRecord : clustersData) {
            if (clusterRecord.size() < startIndex) {
                startIndex -= clusterRecord.size();
            } else {
                afterSkippedClusterValues = clusterRecord.stream()
                        .sorted(Comparator.comparing(getIdFromRecord))
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
    public Record deleteById(Object id) {
        String realId = String.valueOf(id);
        waitRecordForTransactions(realId);
        Cluster<Record> cluster = findCurrentClusterFromId(realId);
        assert cluster != null;
        String pastFirstKey = cluster.firstKey();
        Record record = cluster.delete(realId);
        //Check for emptiness inside the synchronized block so that during the check no new entry is added to this cluster
        synchronized (data) {
            try {
                if (cluster.isEmpty()) {
                    Files.delete(Path.of(directory.getAbsolutePath() + realId));
                    data.remove(realId);
                } else if (pastFirstKey.equals(realId)) {
                    Files.delete(Path.of(directory.getAbsolutePath() + realId));
                    data.remove(realId);
                    data.put(cluster.firstKey(), cluster);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return record;
    }

    @Override
    public void flush() {
        waitAllRecord();
        synchronized (data) {
            if(needGenerateId) {
                try(DataOutputStream outputStream = new DataOutputStream(
                        new FileOutputStream(new File(directory.getAbsolutePath(), "_sequence.imnorm")))) {
                    outputStream.writeLong(sequence);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            data.entrySet().parallelStream()
                    .filter(e -> e.getValue().isRedacted())
                    .forEach(entry -> {
                        try {
                            PrintWriter printWriter =
                                    new PrintWriter(new File(directory.getAbsolutePath(), entry.getKey()));
                            entry.getValue().findAll().forEach(record -> printWriter.println(gson.toJson(record)));
                            entry.getValue().wasFlush();
                            printWriter.close();
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }
}

