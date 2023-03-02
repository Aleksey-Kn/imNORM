package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
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
                if (!file.getName().equals("_sequence.imnorm")) {
                    tempClusterData = new TreeMap<>();
                    scanner = new Scanner(file);
                    while (scanner.hasNextLine()) {
                        now = gson.fromJson(scanner.nextLine(), type);
                        tempClusterData.put(getIdFromRecord.apply(now), now);
                    }
                    data.put(file.getName(), new Cluster<>(tempClusterData, this));
                    scanner.close();
                }
            }
        } catch (FileNotFoundException e) {
            throw new InternalImnormException(e);
        }
    }

    @Override
    protected synchronized Cluster<Record> findCurrentClusterFromId(String id) {
        Map.Entry<String, Cluster<Record>> entry = data.floorEntry(id);
        if (Objects.isNull(entry)) {
            return null;
        } else {
            return entry.getValue();
        }
    }

    @Override
    protected synchronized Record create(final String id, final Record record) {
        if (data.isEmpty() || data.firstKey().compareTo(id) > 0) {
            data.put(id, new Cluster<>(id, record, this));
        } else {
            Cluster<Record> currentCluster = findCurrentClusterFromId(id);
            assert currentCluster != null;
            currentCluster.set(id, record);
            if (currentCluster.size() * sizeOfEntity > CLUSTER_MAX_SIZE) {
                Cluster<Record> newCluster = currentCluster.split();
                data.put(currentCluster.getFirstKey(), newCluster);
            }
        }
        return record;
    }

    @Override
    protected synchronized Record create(final String id, final Record record, final Transaction transaction) {
        if (data.isEmpty() || data.firstKey().compareTo(id) > 0) {
            data.put(id, new Cluster<>(id, record, this, transaction));
        } else {
            Cluster<Record> currentCluster = findCurrentClusterFromId(id);
            assert currentCluster != null;
            currentCluster.set(id, record, transaction);
        }
        return record;
    }

    @Override
    public synchronized Set<Record> findAll() {
        return data.values().stream().map(Cluster::findAll).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public synchronized Set<Record> findAll(Transaction transaction) {
        return data.values().stream()
                .map(recordCluster -> recordCluster.findAll(transaction))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private Set<Record> pagination(List<Collection<Record>> clustersData, int startIndex, int rowCount) {
        List<Record> afterSkippedClusterValues;
        HashSet<Record> result = new HashSet<>(rowCount);
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
    public Set<Record> findAll(int startIndex, int rowCount) {
        List<Collection<Record>> clustersData;
        synchronized (this) {
            clustersData = data.values().stream().map(Cluster::findAll).collect(Collectors.toList());
        }
        return pagination(clustersData, startIndex, rowCount);
    }

    @Override
    public Set<Record> findAll(int startIndex, int rowCount, Transaction transaction) {
        List<Collection<Record>> clustersData;
        synchronized (this) {
            clustersData = data.values().stream()
                    .map(recordCluster -> recordCluster.findAll(transaction))
                    .collect(Collectors.toList());
        }
        return pagination(clustersData, startIndex, rowCount);
    }

    @Override
    public synchronized Record deleteById(Object id) {
        String realId = String.valueOf(id);
        Cluster<Record> cluster = findCurrentClusterFromId(realId);
        if (Objects.isNull(cluster)) {
            return null;
        }
        Record record = cluster.delete(realId);
        try {
            if (cluster.isEmpty()) {
                Files.delete(Path.of(directory.getAbsolutePath(), cluster.getFirstKey()));
                data.remove(cluster.getFirstKey());
            }
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
        return record;
    }

    @Override
    public Record deleteById(Object id, Transaction transaction) {
        String realId = String.valueOf(id);
        Cluster<Record> cluster = findCurrentClusterFromId(realId);
        if (Objects.isNull(cluster)) {
            return null;
        }
        return cluster.delete(realId, transaction);
    }

    @Override
    public void deleteAll() {
        super.deleteAll();
        data.clear();
    }

    @Override
    public synchronized void flush() {
        if (needGenerateId) {
            try (DataOutputStream outputStream = new DataOutputStream(
                    new FileOutputStream(new File(directory.getAbsolutePath(), "_sequence.imnorm")))) {
                outputStream.writeLong(sequence);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        data.forEach((id, cluster) -> cluster.flush(new File(directory.getAbsolutePath(), String.valueOf(id)), gson));
    }
}

