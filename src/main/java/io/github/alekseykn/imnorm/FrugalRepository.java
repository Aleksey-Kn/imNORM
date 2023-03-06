package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Repository with partial unloading clusters in RAM
 *
 * @param <Record> Type of entity for this repository
 * @author Aleksey-Kn
 */
public final class FrugalRepository<Record> extends Repository<Record> {
    /**
     * Set of exists clusters in file data storage
     */
    private final TreeSet<String> clusterNames = new TreeSet<>();

    /**
     * Set of uploaded cluster in RAM
     */
    private final LinkedHashMap<String, Cluster<Record>> openClusters;

    /**
     * Max clusters quantity, which repository can upload in RAM
     */
    private final int maxClustersQuantity;

    /**
     * Find all clusters names, which exists in current directory
     *
     * @param type                Type of entry for this repository
     * @param directory           Directory, contains clusters for this repository
     * @param maxClustersQuantity Max clusters quantity, which repository can upload in RAM
     */
    FrugalRepository(final Class<Record> type, final File directory, final int maxClustersQuantity) {
        super(type, directory);
        this.maxClustersQuantity = maxClustersQuantity;
        assert maxClustersQuantity > 1;
        openClusters = new LinkedHashMap<>(maxClustersQuantity + 1);
        clusterNames.addAll(Arrays.asList(Objects.requireNonNull(directory.list((dir, name) ->
                !name.equals("_sequence.imnorm")))));
    }

    /**
     * Find cluster, which can contains current id. If such cluster not exists in RAM, upload it from file data storage
     *
     * @param id Record id, for which execute search
     * @return Cluster, which can contains current id, or null, if such cluster not contains in data storage
     */
    @Override
    protected synchronized Cluster<Record> findCurrentClusterFromId(final String id) {
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

    /**
     * Add new record to repository. If not exist suitable cluster, a new cluster is being created.
     * If cluster too large, split it on two cluster.
     *
     * @param id     String interpretation of id
     * @param record The record being added to data storage
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    protected synchronized void create(final String id, final Record record) {
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
    }

    /**
     * Add new record to repository. If not exist suitable cluster, a new cluster is being created.
     *
     * @param id     String interpretation of id
     * @param record The record being added to data storage
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    protected synchronized void create(final String id, final Record record, final Transaction transaction) {
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
    }

    /**
     * Read data from not exists in RAM clusters. Used for findAll methods.
     *
     * @return Records from ot exists in RAM clusters
     */
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

    /**
     * Find all data, contains in this repository
     *
     * @return All record, contains in this repository
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized Set<Record> findAll() {
        Set<Record> result = openClusters.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll().stream())
                .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters());
        return result;
    }

    /**
     * Find all data, contains in this repository with current transaction
     *
     * @param transaction Transaction, in which execute save
     * @return All record, contains in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized Set<Record> findAll(final Transaction transaction) {
        Set<Record> result = openClusters.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll(transaction).stream())
                .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters());
        return result;
    }

    /**
     * Pagination data for findAll method
     *
     * @param clusterRecords Data for pagination
     * @param startIndex     Quantity skipped records from start collection
     * @param rowCount       Record quantity, which need return
     * @return Part of the input data obtained based on pagination conditions
     * @throws DeadLockException Current record lock from other transaction
     */
    private List<Record> pagination(final Stream<Record> clusterRecords, final int startIndex, final int rowCount) {
        return clusterRecords
                .sorted(Comparator.comparing(getIdFromRecord))
                .skip(startIndex)
                .limit(rowCount)
                .collect(Collectors.toList());
    }

    /**
     * Find all records with pagination in this repository
     *
     * @param startIndex Quantity skipped records from start collection
     * @param rowCount   Record quantity, which need return
     * @return All record, contains in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized Set<Record> findAll(int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        int currentClusterSize;
        try {
            for (String clusterName : clusterNames) {
                if (openClusters.containsKey(clusterName)) {
                    currentClusterSize = openClusters.get(clusterName).size();
                } else {
                    currentClusterSize = (int) Files.lines(Path.of(directory.getAbsolutePath(), clusterName)).count();
                }
                if (currentClusterSize < startIndex) {
                    startIndex -= currentClusterSize;
                } else {
                    afterSkippedClusterValues = pagination(
                            openClusters.containsKey(clusterName)
                                    ? openClusters.get(clusterName).findAll().stream()
                                    : Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                                    .map(s -> gson.fromJson(s, type)),
                            startIndex,
                            rowCount);
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

    /**
     * Find all records with pagination in current repository with current transaction
     *
     * @param startIndex  Quantity skipped records from start collection
     * @param rowCount    Record quantity, which need return
     * @param transaction Transaction, in which execute find
     * @return All record, contains in current transaction in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll(int startIndex, int rowCount, final Transaction transaction) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        int currentClusterSize;
        try {
            for (String clusterName : clusterNames) {
                if (openClusters.containsKey(clusterName)) {
                    currentClusterSize = openClusters.get(clusterName).sizeWithTransaction();
                } else {
                    currentClusterSize = (int) Files.lines(Path.of(directory.getAbsolutePath(), clusterName)).count();
                }
                if (currentClusterSize < startIndex) {
                    startIndex -= currentClusterSize;
                } else {
                    afterSkippedClusterValues = pagination(
                            openClusters.containsKey(clusterName)
                                    ? openClusters.get(clusterName).findAll(transaction).stream()
                                    : Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                                    .map(s -> gson.fromJson(s, type)),
                            startIndex,
                            rowCount);
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

    /**
     * Remove record with current id. If current cluster becomes empty it is deleted.
     *
     * @param id Id of the record being deleted
     * @return Record, which was deleted from repository, or null, if specified record not exist
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized Record deleteById(final Object id) {
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

    /**
     * Remove record with current id in current transaction
     *
     * @param id Id of the record being deleted
     * @return Record, which was deleted from repository in current transaction, or null,
     * if specified record not exist in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Record deleteById(final Object id, final Transaction transaction) {
        String realId = String.valueOf(id);
        Cluster<Record> cluster = findCurrentClusterFromId(realId);
        if (Objects.isNull(cluster)) {
            return null;
        }
        return cluster.delete(realId, transaction);
    }

    /**
     * Clear current repository from file system and RAM
     *
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public void deleteAll() {
        super.deleteAll();
        clusterNames.clear();
        openClusters.clear();
    }

    /**
     * Save data from current repository to file system and remove all clusters from RAM
     */
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

    /**
     * Drop from RAM after save to file system the most previously opened cluster, which not contains open transaction,
     * if quantity of clusters more max value
     */
    private void checkAndDrop() {
        if (openClusters.size() > maxClustersQuantity) {
            Iterator<Map.Entry<String, Cluster<Record>>> it = openClusters.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Cluster<Record>> entry = it.next();
                if (entry.getValue().hasNotOpenTransactions()) {
                    entry.getValue().flush(new File(directory.getAbsolutePath(), entry.getKey()), gson);
                    it.remove();
                    break;
                }
            }
        }
    }
}
