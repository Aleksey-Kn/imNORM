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
            Record now;
            if (Objects.isNull(clusterId)) {
                return null;
            }
            try (Scanner scanner = new Scanner(new File(directory.getAbsolutePath(), clusterId))) {
                TreeMap<String, Record> tempClusterData = new TreeMap<>();
                while (scanner.hasNextLine()) {
                    now = gson.fromJson(scanner.nextLine(), type);
                    tempClusterData.put(getIdFromRecord.apply(now), now);
                }
                openClusters.put(clusterId, new Cluster<>(tempClusterData));
                checkAndDrop();
                return openClusters.get(clusterId);
            } catch (FileNotFoundException e) {
                throw new InternalImnormException(e);
            }
        }
    }

    @Override
    protected synchronized Record create(String id, Record record) {
        if (needGenerateId) {
            id = generateAndSetIdForRecord(record);
        }
        if (clusterNames.isEmpty() || clusterNames.first().compareTo(id) > 0) {
            Cluster<Record> cluster = new Cluster<>(id, record);
            openClusters.put(id, cluster);
            clusterNames.add(id);
        } else {
            Cluster<Record> currentCluster = findCurrentClusterFromId(id);
            assert currentCluster != null;
            currentCluster.set(id, record);
            if (currentCluster.size() * sizeOfEntity > 100_000) {
                Cluster<Record> newCluster = currentCluster.split();
                String firstKeyNewCluster = newCluster.firstKey();
                openClusters.put(firstKeyNewCluster, newCluster);
                clusterNames.add(firstKeyNewCluster);
            }
        }
        checkAndDrop();
        return record;
    }

    @Override
    public Set<Record> findAll() {
        waitAllRecords();
        synchronized (this) {
            return clusterNames.stream()
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
    }

    @Override
    public Set<Record> findAll(int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        Stream<Record> clusterRecords;
        waitAllRecords();
        try {
            synchronized (this) {
                for (String clusterName : clusterNames) {
                    clusterRecords = Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                            .map(s -> gson.fromJson(s, type));
                    if (clusterRecords.count() < startIndex) {
                        startIndex -= clusterRecords.count();
                    } else {
                        afterSkippedClusterValues = clusterRecords
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
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
    }

    @Override
    public Record deleteById(Object id) {
        String realId = String.valueOf(id);
        waitRecord(realId);
        synchronized (this) {
            Cluster<Record> cluster = findCurrentClusterFromId(realId);
            if (Objects.isNull(cluster)) {
                return null;
            }
            String pastFirstKey = cluster.firstKey();
            Record record = cluster.delete(realId);
            try {
                if (cluster.isEmpty()) {
                    Files.delete(Path.of(directory.getAbsolutePath(), realId));
                    clusterNames.remove(realId);
                    openClusters.remove(realId);
                } else if (pastFirstKey.equals(realId)) {
                    Files.delete(Path.of(directory.getAbsolutePath(), realId));
                    clusterNames.remove(realId);
                    clusterNames.add(cluster.firstKey());
                    openClusters.put(cluster.firstKey(), openClusters.remove(realId));
                }
            } catch (IOException e) {
                throw new InternalImnormException(e);
            }
            return record;
        }
    }

    @Override
    public void flush() {
        waitAllRecords();
        synchronized (this) {
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
    }

    private void checkAndDrop() {
        if (openClusters.size() > maxClusterCount) {
            Iterator<Map.Entry<String, Cluster<Record>>> it = openClusters.entrySet().iterator();
            Map.Entry<String, Cluster<Record>> entry = it.next();
            entry.getValue().flush(new File(directory.getAbsolutePath(), entry.getKey()), gson);
            it.remove();
        }
    }
}
