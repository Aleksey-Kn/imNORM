package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.DeadLockException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Block for keeping records. All clusters correspond to some files from the file data storage.
 * The cluster monitors the changes of its data to track the need for overwriting to disk.
 * Clusters realise transactional behavior: if a transaction affects the current cluster,
 * it creates a copy of its data for later use as the main data in the case of a commit.
 *
 * @param <Record> Type of entity for this cluster
 * @author Aleksey-Kn
 */
public final class Cluster<Record> {
    /**
     * Repository, to which belongs this cluster
     */
    @Getter(AccessLevel.PACKAGE)
    private final Repository<Record> repository;

    /**
     * Copy of records for transactional call. Will be written as basic data in case commit transaction,
     * will be dropped in case rollback transaction.
     */
    private TreeMap<String, Record> copyDataForTransactions = null;

    /**
     * Indicator of changes in the cluster. It is needed for tracking the need to write to disk.
     */
    private boolean redacted = true;

    /**
     * Matching records and their string identifier
     */
    private TreeMap<String, Record> data = new TreeMap<>();

    /**
     * The minimum identifier allowed for storage in this cluster
     */
    private final String firstKey;

    /**
     * Count transaction is waiting for this cluster to be released
     */
    private int waitingTransactionCount = 0;

    /**
     * ID of whether this cluster is active
     */
    @Setter
    private boolean dropped = false;

    /**
     * Create cluster with current records collection
     *
     * @param firstKey Identity of cluster
     * @param map   Record collection
     * @param owner Repository, to which belongs this cluster
     */
    Cluster(final String firstKey, final TreeMap<String, Record> map, final Repository<Record> owner) {
        data = map;
        repository = owner;
        this.firstKey = firstKey;
    }

    /**
     * Create cluster with current records collection in current transaction
     *
     * @param firstKey Identity of cluster
     * @param map         Record collection
     * @param owner       Repository, to which belongs this cluster
     * @param transaction Transaction, in which create cluster
     */
    Cluster(final String firstKey, final TreeMap<String, Record> map, final Repository<Record> owner,
            final Transaction transaction) {
        copyDataForTransactions = map;
        repository = owner;
        this.firstKey = firstKey;

        transaction.captureLock(this);
    }

    /**
     * Create cluster with current record
     *
     * @param id     String identifier, appropriate current record
     * @param record Current record for save in cluster
     * @param owner  Repository, to which belongs this cluster
     */
    Cluster(final String id, Record record, final Repository<Record> owner) {
        data.put(id, record);
        repository = owner;
        this.firstKey = id;
    }

    /**
     * Create cluster with current record in current transaction
     *
     * @param id          String identifier, appropriate current record
     * @param record      Current record for save in cluster
     * @param owner       Repository, to which belongs this cluster
     * @param transaction The transaction to which this record will belong
     */
    Cluster(final String id, final Record record, final Repository<Record> owner, final Transaction transaction) {
        copyDataForTransactions = new TreeMap<>();
        copyDataForTransactions.put(id, record);
        repository = owner;
        this.firstKey = id;

        transaction.captureLock(this);
    }

    /**
     * Add or update record on string identifier and marks the cluster as modified
     *
     * @param key    String identifier
     * @param record Record, which will be put on current string identifier
     * @throws DeadLockException Current record lock from other transaction
     */
    void set(final String key, final Record record) {
        waitAndCheckDeadLock();
        if(dropped) {
            repository.save(record);
            return;
        }

        redacted = true;
        data.put(key, record);
    }

    /**
     * Add or update record on string identifier in current transaction
     *
     * @param key         String identifier
     * @param record      Record, which will be put on current string identifier
     * @param transaction Transaction, in which execute setting
     * @throws DeadLockException Current record lock from other transaction
     */
    void set(final String key, final Record record, final Transaction transaction) {
        lock(transaction);
        if(dropped) {
            repository.save(record, transaction);
            return;
        }

        copyDataForTransactions.put(key, record);
    }

    /**
     * Find record on string identifier
     *
     * @param key String identifier
     * @return Found record or null, if record with current key not exists
     * @throws DeadLockException Current record lock from other transaction
     */
    Record get(final String key) {
        waitAndCheckDeadLock();
        return data.get(key);
    }

    /**
     * Find record on string identifier
     *
     * @param key         String identifier
     * @param transaction Transaction, in which execute find
     * @return Found record or null, if record with current key not exists
     * @throws DeadLockException Current record lock from other transaction
     */
    Record get(final String key, final Transaction transaction) {
        lock(transaction);
        return copyDataForTransactions.get(key);
    }

    /**
     * Find all record from this cluster
     *
     * @return All record from this cluster
     * @throws DeadLockException Current record lock from other transaction
     */
    Collection<Record> findAll() {
        waitAndCheckDeadLock();
        return data.values();
    }

    /**
     * Find all record from this cluster in current transaction
     *
     * @param transaction Transaction, in which execute find
     * @return All record from this cluster in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    Collection<Record> findAll(final Transaction transaction) {
        lock(transaction);
        return copyDataForTransactions.values();
    }

    /**
     * Delete record on string identifier and marks the cluster as modified, if the record existed
     *
     * @param key String identifier
     * @return Deleted record or null, if string identifier not exists
     * @throws DeadLockException Current record lock from other transaction
     */
    Record delete(final String key) {
        waitAndCheckDeadLock();
        if(dropped) {
            return repository.innerDelete(key).orElse(null);
        }

        Record record = data.remove(key);
        if (Objects.nonNull(record))
            redacted = true;
        return record;
    }

    /**
     * Delete record on string identifier in current transactional
     *
     * @param key         String identifier
     * @param transaction Transaction, in which execute delete
     * @return Deleted record or null, if string identifier not exists
     * @throws DeadLockException Current record lock from other transaction
     */
    Record delete(final String key, final Transaction transaction) {
        lock(transaction);
        if(dropped) {
            return repository.innerDelete(key, transaction).orElse(null);
        }

        return copyDataForTransactions.remove(key);
    }

    /**
     * @return Quantity record in this cluster
     */
    int size() {
        return data.size();
    }

    /**
     * @return Quantity record in this cluster in current transaction
     */
    int sizeWithTransaction() {
        return Objects.isNull(copyDataForTransactions) ? size() : copyDataForTransactions.size();
    }

    /**
     * @return The minimum identifier allowed for storage in this cluster
     */
    String getFirstKey() {
        return firstKey;
    }

    /**
     * Checking the contents of a record records with current string identifier
     *
     * @param key String identifier
     * @return True if cluster contains records with current string identifier
     */
    boolean containsKey(final String key) {
        return data.containsKey(key);
    }

    /**
     * @return True if cluster not contains records
     */
    boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Separates half of the records of the current cluster into a new cluster. Used for maximum cluster size limits.
     *
     * @return New cluster, in which a part of the records of the current cluster was taken out
     */
    Cluster<Record> split() {
        Spliterator<Map.Entry<String, Record>> spliterator = data.entrySet().spliterator();
        Spliterator<Map.Entry<String, Record>> currentClusterData = spliterator.trySplit();

        data = StreamSupport.stream(currentClusterData, false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (firstKey, secondKey) -> firstKey,
                        TreeMap::new));

        TreeMap<String, Record> newClusterData = StreamSupport.stream(spliterator, false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (firstKey, secondKey) -> firstKey,
                        TreeMap::new));

        return new Cluster<>(newClusterData.firstKey(), newClusterData, repository);
    }

    /**
     * Save to file data storage records from this cluster
     */
    void flush() {
        if (redacted) {
            try (PrintWriter printWriter = new PrintWriter(new File(repository.directory.getAbsolutePath(), firstKey))) {
                data.forEach((key, record) ->
                        printWriter.println(key.concat(":").concat(repository.gson.toJson(record))));
                redacted = false;
            } catch (FileNotFoundException e) {
                throw new InternalImnormException(e);
            }
        }
    }

    /**
     * Checks for interaction with this cluster from other transactions.
     * If it exists, it waits for it to end and throws an error if the wait has exceeded the maximum allowed time.
     *
     * @throws DeadLockException The maximum waiting time has been exceeded
     */
    private void waitAndCheckDeadLock() {
        if (Objects.nonNull(copyDataForTransactions)) {
            try {
                waitingTransactionCount++;
                repository.wait(1000);
                waitingTransactionCount--;
            } catch (InterruptedException e) {
                throw new InternalImnormException(e);
            }
            if (Objects.nonNull(copyDataForTransactions))
                throw new DeadLockException(firstKey);
        }
    }

    /**
     * Checks whether the current transaction owns this cluster.
     * If he does not own, checks for interaction with this cluster from other transactions.
     * If it not exists, transaction captures this cluster.
     * If it exists, it waits for it to end.
     * Rollback transaction and throws an error if the wait has exceeded the maximum allowed time.
     *
     * @param transaction A transaction that checks or tries to get a lock
     */
    private void lock(final Transaction transaction) {
        synchronized (repository) {
            if (!transaction.lockOwner(this)) {
                if (Objects.nonNull(copyDataForTransactions)) {
                    try {
                        waitingTransactionCount++;
                        long waitLimit = System.currentTimeMillis() + transaction.getWaitTime();
                        while (System.currentTimeMillis() < waitLimit && Objects.nonNull(copyDataForTransactions)) {
                            repository.wait(50);
                        }
                        waitingTransactionCount--;
                    } catch (InterruptedException e) {
                        throw new InternalImnormException(e);
                    }
                    if (Objects.nonNull(copyDataForTransactions)) {
                        transaction.rollback();
                        throw new DeadLockException(firstKey);
                    }
                }
                copyDataForTransactions = new TreeMap<>(data);
                transaction.captureLock(this);
            }
        }
    }

    /**
     * Saving changes made in a transaction and subsequent checking of the cluster for emptiness or overcrowding
     */
    void commit() {
        synchronized (repository) {
            data = copyDataForTransactions;
            copyDataForTransactions = null;
            redacted = true;
            if (waitingTransactionCount == 0) {
                repository.splitClusterIfNeed(this);
                repository.deleteClusterIfNeed(this);
            }
            repository.notifyAll();
        }
    }

    /**
     * Canceling changes made in a transaction
     */
    void rollback() {
        synchronized (repository) {
            copyDataForTransactions = null;
            repository.notifyAll();
        }
    }

    /**
     * @return True, if cluster contains open transaction
     */
    boolean hasNotOpenTransactions() {
        return Objects.isNull(copyDataForTransactions);
    }
}
