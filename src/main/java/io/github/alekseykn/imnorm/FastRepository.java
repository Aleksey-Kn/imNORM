package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import io.github.alekseykn.imnorm.exceptions.DeadLockException;
import lombok.extern.java.Log;

/**
 * Repository with full unloading clusters in RAM
 *
 * @param <Record> Type of entity for this repository
 * @author Aleksey-Kn
 */
@Log
public final class FastRepository<Record> extends Repository<Record> {
    /**
     * The search tree where clusters and their initial keys are mapped
     */
    public final TreeMap<String, Cluster<Record>> data = new TreeMap<>();

    /**
     * Load clusters from file system to RAM
     *
     * @param type      Type of data entity
     * @param directory The directory where the clusters are saved
     */
    FastRepository(final Class<Record> type, final File directory) {
        super(type, directory);
        Scanner scanner;
        Record now;
        TreeMap<String, Record> tempClusterData;
        try {
            for (File file : Objects.requireNonNull(directory.listFiles((dir, name) ->
                    !name.equals("_sequence.imnorm")))) {
                tempClusterData = new TreeMap<>();
                scanner = new Scanner(file);
                while (scanner.hasNextLine()) {
                    now = gson.fromJson(scanner.nextLine(), type);
                    tempClusterData.put(getIdFromRecord.apply(now), now);
                }
                data.put(file.getName(), new Cluster<>(tempClusterData, this));
                scanner.close();
            }
        } catch (FileNotFoundException e) {
            throw new InternalImnormException(e);
        }
    }

    /**
     * Find cluster, which can contains current id
     *
     * @param id Record id, for which execute search
     * @return Cluster, which can contains current id, or null, if such cluster not contains in data storage
     */
    @Override
    protected synchronized Cluster<Record> findCurrentClusterFromId(final String id) {
        Map.Entry<String, Cluster<Record>> entry = data.floorEntry(id);
        if (Objects.isNull(entry)) {
            return null;
        } else {
            return entry.getValue();
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
        if (data.isEmpty() || data.firstKey().compareTo(id) > 0) {
            data.put(id, new Cluster<>(id, record, this));
        } else {
            Cluster<Record> currentCluster = findCurrentClusterFromId(id);
            assert currentCluster != null;
            currentCluster.set(id, record);
            splitClusterIfNeed(currentCluster);
        }
    }

    /**
     * Add new record to repository. If not exist suitable cluster, a new cluster is being created.
     *
     * @param id          String interpretation of id
     * @param record      The record being added to data storage
     * @param transaction Transaction, in which execute create
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    protected synchronized void create(final String id, final Record record, final Transaction transaction) {
        if (data.isEmpty() || data.firstKey().compareTo(id) > 0) {
            data.put(id, new Cluster<>(id, record, this, transaction));
        } else {
            Cluster<Record> currentCluster = findCurrentClusterFromId(id);
            assert currentCluster != null;
            currentCluster.set(id, record, transaction);
        }
    }

    /**
     * Find all data, contains in this repository
     *
     * @return All record, contains in this repository
     * @throws DeadLockException Current record lock from other transaction
     */
    @Override
    public synchronized Set<Record> findAll() {
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
    public synchronized Set<Record> findAll(final Transaction transaction) {
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

    @Override
    protected synchronized void deleteClusterIfNeed(Cluster<Record> cluster) {
        try {
            if (cluster.isEmpty()) {
                log.info("Cluster " + cluster.getFirstKey() + " removed!");
                Files.delete(Path.of(directory.getAbsolutePath(), cluster.getFirstKey()));
                data.remove(cluster.getFirstKey());
            }
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
    }

    @Override
    protected synchronized void splitClusterIfNeed(Cluster<Record> cluster) {
        if (cluster.size() * sizeOfEntity > CLUSTER_MAX_SIZE) {
            Cluster<Record> newCluster = cluster.split();
            data.put(cluster.getFirstKey(), newCluster);
            log.info("Cluster " + cluster.getFirstKey() + " split on " + newCluster.getFirstKey());
        }
    }
}
