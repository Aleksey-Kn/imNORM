package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.DeadLockException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;
import io.github.alekseykn.imnorm.where.Condition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Repository with partial unloading clusters in RAM
 *
 * @param <Record> Type of entity for this repository
 * @author Aleksey-Kn
 */
public class FrugalRepository<Record> extends Repository<Record> {
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
     * @param id             Record id, for which execute search
     * @param clusterCreator Algorithm create of cluster
     * @return Cluster, which can contains current id, or null, if such cluster not contains in data storage
     */
    private synchronized Optional<Cluster<Record>> findCurrentClusterFromId(final String id,
                                                                            final BiFunction<String, TreeMap<String, Record>, Cluster<Record>> clusterCreator) {
        String clusterId = clusterNames.floor(id);
        if (openClusters.containsKey(clusterId)) {
            return Optional.of(openClusters.get(clusterId));
        } else {
            if (Objects.isNull(clusterId)) {
                return Optional.empty();
            } else {
                Path clusterPath = Path.of(directory.getAbsolutePath(), clusterId);
                try (Stream<String> stream = Files.lines(clusterPath)) {
                    TreeMap<String, Record> tempClusterData = new TreeMap<>();
                    stream.forEach(line -> {
                        int index = line.indexOf(':');
                        tempClusterData.put(line.substring(0, index),
                                gson.fromJson(line.substring(index + 1), type));
                    });
                    if (tempClusterData.isEmpty()) {
                        Files.delete(clusterPath);
                        return Optional.empty();
                    } else {
                        openClusters.put(clusterId, clusterCreator.apply(clusterId, tempClusterData));
                        checkAndDropIfTooMuchOpenClusters();
                        return Optional.of(openClusters.get(clusterId));
                    }
                } catch (IOException e) {
                    throw new InternalImnormException(e);
                }
            }
        }
    }

    /**
     * Find cluster, which can contain record with current id
     *
     * @param id Record id, for which execute search
     * @return Cluster, which can contain current record
     */
    @Override
    protected synchronized Optional<Cluster<Record>> findCurrentClusterFromId(final String id) {
        return findCurrentClusterFromId(id,
                (firstKey, tempClusterData) -> new Cluster<>(firstKey, tempClusterData, this));
    }

    /**
     * Find cluster, which can contain record with current id, in transaction context
     *
     * @param id          Record id, for which execute search
     * @param transaction Current transaction
     * @return Cluster, which can contain current record
     */
    private synchronized Optional<Cluster<Record>> findCurrentClusterFromId(final String id, final Transaction transaction) {
        return findCurrentClusterFromId(id,
                (firstKey, tempClusterData) -> new Cluster<>(firstKey, tempClusterData, this, transaction));
    }

    /**
     * Add new cluster and insert current record in it
     *
     * @param id     String interpretation of id
     * @param record The record being added to data storage
     */
    @Override
    protected synchronized void createClusterForRecord(final String id, final Record record) {
        openClusters.put(id, new Cluster<>(id, id, record, this));
        clusterNames.add(id);
        checkAndDropIfTooMuchOpenClusters();
    }

    /**
     * Add new cluster and insert current record in current transaction
     *
     * @param id          String interpretation of id
     * @param record      The record being added to data storage
     * @param transaction Transaction, in which execute create
     */
    @Override
    protected synchronized void createClusterForRecord(final String id, final Record record,
                                                       final Transaction transaction) {
        openClusters.put(id, new Cluster<>(id, id, record, this, transaction));
        clusterNames.add(id);
        checkAndDropIfTooMuchOpenClusters();
    }

    /**
     * Add new record if record with current id not exist in data storage.
     * Update record if current id exist in data storage. Changes execute in current transaction.
     *
     * @param record      Record for save
     * @param transaction Transaction, in which execute save
     * @return Record with new id, if auto-generate on and record with current id not exist in data storage,
     * else return inputted record
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized Record save(Record record, Transaction transaction) {
        checkForBlocking();
        String id = needGenerateIdForRecord(record) ? generateAndSetIdForRecord(record) : getIdFromRecord(record);
        findCurrentClusterFromId(id, transaction).ifPresentOrElse(cluster -> cluster.set(id, record, transaction),
                () -> createClusterForRecord(id, record, transaction));

        return record;
    }

    /**
     * Add new cluster and insert current collection in it
     *
     * @param records Records, for which needed to create new cluster
     */
    @Override
    protected synchronized Cluster<Record> createClusterForRecords(List<Record> records) {
        Cluster<Record> cluster = super.createClusterForRecords(records);
        openClusters.put(cluster.getFirstKey(), cluster);
        clusterNames.add(cluster.getFirstKey());
        splitClusterIfNeed(cluster);
        checkAndDropIfTooMuchOpenClusters();

        return cluster;
    }

    /**
     * Add new cluster and insert current collection in current transaction
     *
     * @param records     Records, for which needed to create new cluster
     * @param transaction Transaction, in which execute create
     */
    @Override
    protected synchronized Cluster<Record> createClusterForRecords(List<Record> records, Transaction transaction) {
        Cluster<Record> cluster = super.createClusterForRecords(records, transaction);
        openClusters.put(cluster.getFirstKey(), cluster);
        clusterNames.add(cluster.getFirstKey());
        splitClusterIfNeed(cluster);
        checkAndDropIfTooMuchOpenClusters();
        return cluster;
    }

    /**
     * Read data from not exists in RAM clusters. Used for findAll methods.
     *
     * @return Records from ot exists in RAM clusters
     */
    private Stream<Record> findRecordFromNotOpenClusters() {
        return clusterNames.parallelStream()
                .filter(clusterName -> !openClusters.containsKey(clusterName))
                .flatMap(clusterName -> {
                    try {
                        return Files.lines(Path.of(directory.getAbsolutePath(), clusterName));
                    } catch (IOException e) {
                        throw new InternalImnormException(e);
                    }
                })
                .map(s -> s.substring(s.indexOf(':') + 1))
                .map(record -> gson.fromJson(record, type));
    }

    /**
     * Find all data, contains in this repository
     *
     * @return All record, contains in this repository
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll() {
        Set<Record> result = openClusters.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll().stream())
                .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters().collect(Collectors.toSet()));
        return result;
    }

    /**
     * Find all data, contains in this repository with current transaction
     *
     * @param transaction Transaction, in which execute find
     * @return All record, contains in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll(final Transaction transaction) {
        Set<Record> result = openClusters.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll(transaction).stream())
                .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters().collect(Collectors.toSet()));
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
                .sorted(Comparator.comparing(this::getIdFromRecord))
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
    public Set<Record> findAll(int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        int currentClusterSize;
        List<String> readLines = null;

        try {
            for (String clusterName : clusterNames) {
                if (openClusters.containsKey(clusterName)) {
                    currentClusterSize = openClusters.get(clusterName).size();
                } else {
                    readLines = Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                            .collect(Collectors.toList());
                    currentClusterSize = readLines.size();
                }
                if (currentClusterSize < startIndex) {
                    startIndex -= currentClusterSize;
                } else {
                    afterSkippedClusterValues = pagination(openClusters.containsKey(clusterName)
                                    ? openClusters.get(clusterName).findAll().stream()
                                    : Objects.requireNonNull(readLines).stream()
                                    .map(s -> s.substring(s.indexOf(':') + 1))
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
        List<String> readLines = null;

        try {
            for (String clusterName : clusterNames) {
                if (openClusters.containsKey(clusterName)) {
                    currentClusterSize = openClusters.get(clusterName).sizeWithTransaction();
                } else {
                    readLines = Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                            .collect(Collectors.toList());
                    currentClusterSize = readLines.size();
                }
                if (currentClusterSize < startIndex) {
                    startIndex -= currentClusterSize;
                } else {
                    afterSkippedClusterValues = pagination(openClusters.containsKey(clusterName)
                                    ? openClusters.get(clusterName).findAll(transaction).stream()
                                    : Objects.requireNonNull(readLines).stream()
                                    .map(s -> s.substring(s.indexOf(':') + 1))
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
     * Find all records in current repository, suitable for the specified condition
     *
     * @param condition Condition for search
     * @return All records, suitable for the specified condition
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll(Condition<Record> condition) {
        Set<Record> result = openClusters.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll().stream().filter(condition::fitsCondition))
                .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters().filter(condition::fitsCondition).collect(Collectors.toSet()));
        return result;
    }

    /**
     * Find all records, suitable for the specified condition in current transaction
     *
     * @param condition   Condition for search
     * @param transaction Transaction, in which execute find
     * @return Suitable for the specified condition records, contains in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll(Condition<Record> condition, Transaction transaction) {
        Set<Record> result = openClusters.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll(transaction).stream().filter(condition::fitsCondition))
                .collect(Collectors.toSet());
        result.addAll(findRecordFromNotOpenClusters().filter(condition::fitsCondition).collect(Collectors.toSet()));
        return result;
    }

    /**
     * Find all records with pagination, suitable for the specified condition
     *
     * @param condition  Condition for search
     * @param startIndex Quantity skipped records from start collection
     * @param rowCount   Record quantity, which need return
     * @return Suitable for the specified condition records, contains in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll(Condition<Record> condition, int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> records;

        try {
            for (String clusterName : clusterNames) {
                records = (openClusters.containsKey(clusterName)
                        ? openClusters.get(clusterName).findAll().stream()
                        : Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                        .map(s -> s.substring(s.indexOf(':') + 1))
                        .map(s -> gson.fromJson(s, type)))
                        .filter(condition::fitsCondition)
                        .collect(Collectors.toList());
                if (records.size() < startIndex) {
                    startIndex -= records.size();
                } else {
                    records = pagination(records.stream(), startIndex, rowCount);
                    result.addAll(records);

                    rowCount -= records.size();
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
     * Find all records with pagination, suitable for the specified condition in current transaction
     *
     * @param condition   Condition for search
     * @param startIndex  Quantity skipped records from start collection
     * @param rowCount    Record quantity, which need return
     * @param transaction Transaction, in which execute find
     * @return Suitable for the specified condition records, contains in current transaction in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll(Condition<Record> condition, int startIndex, int rowCount, Transaction transaction) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> records;

        try {
            for (String clusterName : clusterNames) {
                records = (openClusters.containsKey(clusterName)
                        ? openClusters.get(clusterName).findAll(transaction).stream()
                        : Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                        .map(s -> s.substring(s.indexOf(':') + 1))
                        .map(s -> gson.fromJson(s, type)))
                        .filter(condition::fitsCondition)
                        .collect(Collectors.toList());
                if (records.size() < startIndex) {
                    startIndex -= records.size();
                } else {
                    records = pagination(records.stream(), startIndex, rowCount);
                    result.addAll(records);

                    rowCount -= records.size();
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
     * Clear current repository from file system and RAM
     *
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized void deleteAll() {
        super.deleteAll();
        clusterNames.clear();
        openClusters.clear();
    }

    /**
     * Save data from current repository to file system and remove all clusters from RAM
     */
    @Override
    public synchronized void flush() {
        super.flush();
        openClusters.values().forEach(Cluster::flush);
        openClusters.entrySet().removeIf(stringClusterEntry -> stringClusterEntry.getValue().hasNotOpenTransactions());
    }

    /**
     * @return Number of records in the repository
     */
    @Override
    public synchronized long size() {
        return clusterNames.parallelStream()
                .filter(clusterName -> !openClusters.containsKey(clusterName))
                .mapToLong(clusterName -> {
                    try (Stream<String> stream = Files.lines(Path.of(directory.getAbsolutePath(), clusterName))) {
                        return stream.count();
                    } catch (IOException e) {
                        throw new InternalImnormException(e);
                    }
                }).sum()
                + openClusters.values().stream().mapToInt(Cluster::size).sum();
    }

    /**
     * Drop from RAM after save to file system the most previously opened cluster, which not contains open transaction,
     * if quantity of clusters more max value
     */
    private synchronized void checkAndDropIfTooMuchOpenClusters() {
        if (openClusters.size() > maxClustersQuantity) {
            Iterator<Map.Entry<String, Cluster<Record>>> it = openClusters.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Cluster<Record>> entry = it.next();
                if (entry.getValue().hasNotOpenTransactions()) {
                    entry.getValue().flush();
                    it.remove();
                    break;
                }
            }
        }
    }

    @Override
    protected synchronized void splitClusterIfNeed(Cluster<Record> cluster) {
        if (cluster.size() * sizeOfEntity > CLUSTER_MAX_SIZE) {
            Cluster<Record> newCluster = cluster.split();
            String firstKeyNewCluster = newCluster.getFirstKey();
            openClusters.put(firstKeyNewCluster, newCluster);
            clusterNames.add(firstKeyNewCluster);
        }
    }

    @Override
    protected synchronized void deleteClusterIfNeed(Cluster<Record> cluster) {
        if (cluster.isEmpty()) {
            try {
                Files.delete(Path.of(directory.getAbsolutePath(), cluster.getFirstKey()));
            } catch (IOException ignore) {
            }
            clusterNames.remove(cluster.getFirstKey());
            openClusters.remove(cluster.getFirstKey());
        }
    }

    /**
     * Checks the existence of a record with the specified id
     *
     * @param id The id being checked
     * @return True, if record is exist
     */
    @Override
    protected boolean existsById(final Object id) {
        String stringId = getStringHashFromId(id);
        if (stringId.compareTo(clusterNames.first()) > 0) {
            String clusterName = clusterNames.floor(stringId);
            Cluster<Record> cluster = openClusters.get(clusterName);
            if (Objects.nonNull(cluster)) {
                return cluster.containsKey(stringId);
            } else {
                try (Stream<String> stream = Files.lines(Path.of(directory.getPath(), clusterName))) {
                    return stream.map(s -> s.substring(0, s.indexOf(':'))).anyMatch(s -> s.equals(stringId));
                } catch (IOException e) {
                    throw new InternalImnormException(e);
                }
            }
        } else {
            return false;
        }
    }
}
