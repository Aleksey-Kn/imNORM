package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Anyone methods is synchronized, because in them re-assigned 'openedCluster'.
 *
 * @param <Record> Type of entity for this repository
 */
public final class FrugalRepository<Record> extends Repository<Record> {
    private final TreeSet<String> clusterNames = new TreeSet<>();
    private final LinkedHashMap<String, Cluster<Record>> openClusters;
    private final int maxClusterCount;

    FrugalRepository(Class<Record> type, File directory, int maxClusterCount) {
        super(type, directory);
        this.maxClusterCount = maxClusterCount;
        assert maxClusterCount > 1;
        openClusters = new LinkedHashMap<>(maxClusterCount + 1);
    }

    @Override
    protected synchronized Cluster<Record> findCurrentClusterFromId(String id) {
        String clusterId = clusterNames.floor(id);
        if (openClusters.containsKey(clusterId)) {
            return openClusters.get(clusterId);
        } else {
            if (Objects.isNull(clusterId)) {
                return null;
            }
            try {
                TreeMap<String, Record> tempClusterData = new TreeMap<>();
                Files.lines(Path.of(directory.getAbsolutePath(), clusterId)).forEach(line -> {
                    Record now = gson.fromJson(line, type);
                    tempClusterData.put(getIdFromRecord.apply(now), now);
                });
                openClusters.put(clusterId, new Cluster<>(tempClusterData, this));
                checkAndDrop();
                return openClusters.get(clusterId);
            } catch (IOException e) {
                throw new InternalImnormException(e);
            }
        }
    }

    @Override
    protected synchronized Record create(final String id, final Record record) {
        if (clusterNames.isEmpty() || clusterNames.first().compareTo(id) > 0) {
            Cluster<Record> cluster = new Cluster<>(id, record, this);
            openClusters.put(id, cluster);
            clusterNames.add(id);
        } else {
            Cluster<Record> currentCluster = findCurrentClusterFromId(id);
            assert currentCluster != null;
            currentCluster.set(id, record);
            if (currentCluster.size() * sizeOfEntity > CLUSTER_MAX_SIZE) {
                Cluster<Record> newCluster = currentCluster.split();
                String firstKeyNewCluster = newCluster.getFirstKey();
                openClusters.put(firstKeyNewCluster, newCluster);
                clusterNames.add(firstKeyNewCluster);
            }
        }
        checkAndDrop();
        return record;
    }

    @Override
    protected synchronized Record create(final String id, final Record record, final Transaction transaction) {
        if (clusterNames.isEmpty() || clusterNames.first().compareTo(id) > 0) {
            Cluster<Record> cluster = new Cluster<>(id, record, this, transaction);
            openClusters.put(id, cluster);
            clusterNames.add(id);
        } else {
            Cluster<Record> currentCluster = findCurrentClusterFromId(id);
            assert currentCluster != null;
            currentCluster.set(id, record, transaction);
        }
        checkAndDrop();
        return record;
    }

    private Set<Record> findRecordFromNotOpenClusters() {
        return clusterNames.stream()
                .filter(clusterName -> !openClusters.containsKey(clusterName))
                .flatMap(clusterName -> {
                    try {
                        return Files.lines(Path.of(directory.getAbsolutePath(), clusterName));
                    } catch (IOException e) {
                        throw new InternalImnormException(e);
                    }
                })
                .map(record -> gson.fromJson(record, type))
                .collect(Collectors.toSet());
    }

    @Override
    public synchronized Set<Record> findAll() {
        Set<Record> result = openClusters.values().stream()
                        .flatMap(recordCluster -> recordCluster.findAll().stream())
                        .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters());
        return result;
    }

    @Override
    public synchronized Set<Record> findAll(Transaction transaction) {
        Set<Record> result = openClusters.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll(transaction).stream())
                .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters());
        return result;
    }

    private List<Record> findAfterSkippedClusterValues(Stream<Record> clusterRecords, int startIndex, int rowCount) {
        return clusterRecords
                .sorted(Comparator.comparing(getIdFromRecord))
                .skip(startIndex)
                .limit(rowCount)
                .collect(Collectors.toList());
    }

    @Override
    public synchronized Set<Record> findAll(int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        Stream<Record> clusterRecords;
        try {
            for (String clusterName : clusterNames) {
                if(openClusters.containsKey(clusterName)) {
                    clusterRecords = openClusters.get(clusterName).findAll().stream();
                } else {
                    clusterRecords = Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                            .map(s -> gson.fromJson(s, type));
                }
                if (clusterRecords.count() < startIndex) {
                    startIndex -= clusterRecords.count();
                } else {
                    afterSkippedClusterValues = findAfterSkippedClusterValues(clusterRecords, startIndex, rowCount);
                    result.addAll(afterSkippedClusterValues);

                    rowCount -= afterSkippedClusterValues.size();
                    startIndex = 0;
                }
                if (rowCount == 0)
                    break;
            }
            return result;
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
    }

    @Override
    public Set<Record> findAll(int startIndex, int rowCount, Transaction transaction) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        Stream<Record> clusterRecords;
        try {
            for (String clusterName : clusterNames) {
                if(openClusters.containsKey(clusterName)) {
                    clusterRecords = openClusters.get(clusterName).findAll(transaction).stream();
                } else {
                    clusterRecords = Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                            .map(s -> gson.fromJson(s, type));
                }
                if (clusterRecords.count() < startIndex) {
                    startIndex -= clusterRecords.count();
                } else {
                    afterSkippedClusterValues = findAfterSkippedClusterValues(clusterRecords, startIndex, rowCount);
                    result.addAll(afterSkippedClusterValues);

                    rowCount -= afterSkippedClusterValues.size();
                    startIndex = 0;
                }
                if (rowCount == 0)
                    break;
            }
            return result;
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
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
                clusterNames.remove(cluster.getFirstKey());
                openClusters.remove(cluster.getFirstKey());
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
        clusterNames.clear();
        openClusters.clear();
    }

    @Override
    public synchronized void flush() {
        for (Map.Entry<String, Cluster<Record>> entry : openClusters.entrySet()) {
            if (needGenerateId) {
                try (DataOutputStream outputStream = new DataOutputStream(
                        new FileOutputStream(new File(directory.getAbsolutePath(), "_sequence.imnorm")))) {
                    outputStream.writeLong(sequence);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            entry.getValue().flush(new File(directory.getAbsolutePath(), entry.getKey()), gson);
        }
        openClusters.clear();
    }

    private void checkAndDrop() {
        if (openClusters.size() > maxClusterCount) {
            Iterator<Map.Entry<String, Cluster<Record>>> it = openClusters.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Cluster<Record>> entry = it.next();
                if(entry.getValue().hasNotOpenTransactions()) {
                    entry.getValue().flush(new File(directory.getAbsolutePath(), entry.getKey()), gson);
                    it.remove();
                    break;
                }
            }
        }
    }
}
