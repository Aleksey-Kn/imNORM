package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.DeadLockException;
import io.github.alekseykn.imnorm.where.Condition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Repository with full unloading clusters in RAM
 *
 * @param <Record> Type of entity for this repository
 * @author Aleksey-Kn
 */
public class FastRepository<Record> extends Repository<Record> {
    /**
     * The search tree where clusters and their initial keys are mapped
     */
    private final TreeMap<Integer, Cluster<Record>> data = new TreeMap<>();

    /**
     * Load clusters from file system to RAM
     *
     * @param type      Type of data entity
     * @param directory The directory where the clusters are saved
     */
    FastRepository(final Class<Record> type, final File directory) {
        super(type, directory);


        for (File file : Objects.requireNonNull(directory.listFiles((dir, name) ->
                !name.equals("_sequence.imnorm")))) {
            data.put(Integer.parseInt(file.getName()),
                    new Cluster<>(Integer.parseInt(file.getName()), clusterFileManipulator.read(file.toPath()), this));
        }
    }

    /**
     * Find cluster, which can contains current id
     *
     * @param id Record id, for which execute search
     * @return Cluster, which can contains current id, or null, if such cluster not contains in data storage
     */
    @Override
    protected synchronized Optional<Cluster<Record>> findCurrentClusterFromId(final int id) {
        Map.Entry<Integer, Cluster<Record>> entry = data.floorEntry(id);
        if (Objects.isNull(entry)) {
            return Optional.empty();
        } else {
            return Optional.of(entry.getValue());
        }
    }

    /**
     * Add new cluster and insert current record in it
     *
     * @param hash   Hash from id
     * @param id     ID of record
     * @param record The record being added to data storage
     */
    @Override
    protected synchronized void createClusterForRecord(final int hash, final Object id, final Record record) {
        data.put(hash, new Cluster<>(hash, id, record, this));
    }

    /**
     * Add new cluster and insert current record in current transaction
     *
     * @param hash   Hash from id
     * @param id     ID of record
     * @param record      The record being added to data storage
     * @param transaction Transaction, in which execute create
     */
    @Override
    protected synchronized void createClusterForRecord(final int hash, final Object id, final Record record,
                                                       final Transaction transaction) {
        data.put(hash, new Cluster<>(hash, id, record, this, transaction));
    }

    /**
     * Add new cluster and insert current collection in it
     *
     * @param records Records, for which needed to create new cluster
     */
    @Override
    protected synchronized Cluster<Record> createClusterForRecords(final List<Record> records) {
        Cluster<Record> cluster = super.createClusterForRecords(records);
        data.put(cluster.getFirstKey(), cluster);
        splitClusterIfNeed(cluster);
        return cluster;
    }

    /**
     * Add new cluster and insert current collection in current transaction
     *
     * @param records     Records, for which needed to create new cluster
     * @param transaction Transaction, in which execute create
     */
    @Override
    protected synchronized Cluster<Record> createClusterForRecords(final List<Record> records,
                                                                   final Transaction transaction) {
        Cluster<Record> cluster = super.createClusterForRecords(records, transaction);
        data.put(cluster.getFirstKey(), cluster);
        splitClusterIfNeed(cluster);
        return cluster;
    }

    /**
     * Find all data, contains in this repository
     *
     * @return All record, contains in this repository
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll() {
        return data.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll().stream())
                .collect(Collectors.toSet());
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
        return data.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll(transaction).stream())
                .collect(Collectors.toSet());
    }

    /**
     * Pagination data for findAll method
     *
     * @param clustersData Data from all clusters this repository
     * @param startIndex   Quantity skipped records from start collection
     * @param rowCount     Record quantity, which need return
     * @return Part of the repository data obtained based on pagination conditions
     */
    private Set<Record> pagination(final List<Collection<Record>> clustersData, int startIndex, int rowCount) {
        List<Record> afterSkippedClusterValues;
        HashSet<Record> result = new HashSet<>(rowCount);
        for (Collection<Record> clusterRecord : clustersData) {
            if (clusterRecord.size() < startIndex) {
                startIndex -= clusterRecord.size();
            } else {
                afterSkippedClusterValues = clusterRecord.stream()
                        .sorted(Comparator.comparing(this::getHashIdFromRecord))
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

    /**
     * Find all records with pagination in this repository
     *
     * @param startIndex Quantity skipped records from start collection
     * @param rowCount   Record quantity, which need return
     * @return All record, contains in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public Set<Record> findAll(final int startIndex, final int rowCount) {
        List<Collection<Record>> clustersData;
        synchronized (this) {
            clustersData = data.values().stream()
                    .map(Cluster::findAll)
                    .collect(Collectors.toList());
        }
        return pagination(clustersData, startIndex, rowCount);
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
    public Set<Record> findAll(final int startIndex, final int rowCount, final Transaction transaction) {
        List<Collection<Record>> clustersData;
        synchronized (this) {
            clustersData = data.values().stream()
                    .map(recordCluster -> recordCluster.findAll(transaction))
                    .collect(Collectors.toList());
        }
        return pagination(clustersData, startIndex, rowCount);
    }

    /**
     * Find all records in current repository, suitable for the specified condition
     *
     * @param condition Condition for search
     * @return All records, suitable for the specified condition
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized Set<Record> findAll(final Condition<Record> condition) {
        return data.values().stream()
                .flatMap(recordCluster -> recordCluster.findAll().stream())
                .parallel()
                .filter(condition::fitsCondition)
                .collect(Collectors.toSet());
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
    public Set<Record> findAll(final Condition<Record> condition, final Transaction transaction) {
        return data.values().parallelStream()
                .flatMap(recordCluster -> recordCluster.findAll(transaction).stream())
                .filter(condition::fitsCondition)
                .collect(Collectors.toSet());
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
    public Set<Record> findAll(final Condition<Record> condition, final int startIndex, final int rowCount) {
        List<Collection<Record>> clustersData;
        synchronized (this) {
            clustersData = data.values().stream()
                    .map(Cluster::findAll)
                    .map(records -> records.stream().filter(condition::fitsCondition).collect(Collectors.toList()))
                    .collect(Collectors.toList());
        }
        return pagination(clustersData, startIndex, rowCount);
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
    public Set<Record> findAll(final Condition<Record> condition, final int startIndex, final int rowCount,
                               final Transaction transaction) {
        List<Collection<Record>> clustersData;
        synchronized (this) {
            clustersData = data.values().stream()
                    .map(recordCluster -> recordCluster.findAll(transaction))
                    .map(records -> records.stream().filter(condition::fitsCondition).collect(Collectors.toList()))
                    .collect(Collectors.toList());
        }
        return pagination(clustersData, startIndex, rowCount);
    }

    /**
     * Clear current repository from file system and RAM
     *
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized void deleteAll() {
        super.deleteAll();
        data.clear();
    }

    /**
     * Save data from current repository to file system
     */
    @Override
    public synchronized void flush() {
        super.flush();
        data.values().forEach(Cluster::flush);
    }

    /**
     * @return Number of records in the repository
     */
    @Override
    public synchronized long size() {
        return data.values().stream().mapToInt(Cluster::size).sum();
    }

    @Override
    protected synchronized void deleteClusterIfNeed(final Cluster<Record> cluster) {
        if (cluster.isEmpty()) {
            try {
                Files.delete(Path.of(directory.getAbsolutePath(), Integer.toString(cluster.getFirstKey())));
            } catch (IOException ignore) {
            }
            data.remove(cluster.getFirstKey());
        }
    }

    @Override
    protected synchronized void splitClusterIfNeed(final Cluster<Record> cluster) {
        if (needSplit(data.size(), cluster.size())) {
            cluster.split().ifPresent(newCluster -> data.put(newCluster.getFirstKey(), newCluster));
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
        int hash = getHashFromId(id);
        if (hash > data.firstKey())
            return data.floorEntry(hash).getValue().containsKey(hash, id);
        else
            return false;
    }
}
