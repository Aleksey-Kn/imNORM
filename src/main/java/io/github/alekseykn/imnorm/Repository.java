package io.github.alekseykn.imnorm;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.annotations.Id;
import io.github.alekseykn.imnorm.exceptions.CountIdException;
import io.github.alekseykn.imnorm.exceptions.CreateDataStorageException;
import io.github.alekseykn.imnorm.exceptions.IllegalGeneratedIdTypeException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;
import io.github.alekseykn.imnorm.exceptions.RepositoryWasLockedException;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import io.github.alekseykn.imnorm.exceptions.DeadLockException;
import java.util.function.Function;

/**
 * Provides an interface for manipulating with entity current type.
 * The whole collection split on clusters, through which direct access to data is carried out.
 *
 * @param <Record> Type of data entity
 * @author Aleksey-Kn
 */
public abstract class Repository<Record> {
    /**
     * Approximate maximum cluster size
     */
    protected static final int CLUSTER_MAX_SIZE = 10_000;

    /**
     * Entity id field
     */
    protected final Field recordId;

    /**
     * Function for get data entity id as string
     */
    protected final Function<Record, String> getIdFromRecord;

    /**
     * Auto-generation activity flag
     */
    protected final boolean needGenerateId;

    /**
     * The directory where the clusters are saved
     */
    protected final File directory;

    /**
     * Object for parse data entity to json
     */
    protected final Gson gson = new Gson();

    /**
     * Approximate size of one record in json string
     */
    protected final int sizeOfEntity;

    /**
     * Use where Auto-generation on. Contains next id for entity.
     */
    protected long sequence;

    /**
     * Type of data entity
     */
    protected Class<Record> type;
    
    /**
     * Indicator of the possibility of further use of the repository for write data
     */
    protected boolean locked = false;

    /**
     * Analyse data entity type and create directory for clusters
     *
     * @param type      Type of data entity
     * @param directory The directory where the clusters are saved
     */
    protected Repository(final Class<Record> type, final File directory) {
        this.directory = directory;
        this.type = type;

        if (!directory.exists()) {
            if (!directory.mkdir())
                throw new CreateDataStorageException(directory);
        }

        Field[] fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> Objects.nonNull(field.getAnnotation(Id.class)))
                .toArray(Field[]::new);
        if (fields.length != 1)
            throw new CountIdException(type);
        recordId = fields[0];
        recordId.setAccessible(true);
        getIdFromRecord = record -> {
            try {
                return String.valueOf(recordId.get(record));
            } catch (IllegalAccessException e) {
                throw new InternalImnormException(e);
            }
        };
        needGenerateId = recordId.getAnnotation(Id.class).autoGenerate();
        sizeOfEntity = type.getDeclaredFields().length * 50;

        if (needGenerateId) {
            try (DataInputStream fileInputStream = new DataInputStream(
                    new FileInputStream(new File(directory.getAbsolutePath(), "_sequence.imnorm")))) {
                sequence = fileInputStream.readLong();
            } catch (IOException e) {
                sequence = 1;
            }
        }
    }

    /**
     * Create and set id needed type for current record
     *
     * @param record Record, which needed create id
     * @return Created id
     */
    protected String generateAndSetIdForRecord(final Record record) {
        try {
            switch (recordId.getType().getSimpleName()) {
                case "byte", "Byte" -> recordId.set(record, (byte) sequence++);
                case "short", "Short" -> recordId.set(record, (short) sequence++);
                case "int", "Integer" -> recordId.set(record, (int) sequence++);
                case "long", "Long" -> recordId.set(record, sequence++);
                default -> recordId.set(record, Long.toString(sequence++));
            }
            return getIdFromRecord.apply(record);
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        } catch (IllegalArgumentException e) {
            throw new IllegalGeneratedIdTypeException();
        }
    }

    /**
     * Find cluster, which can contain record with current id
     *
     * @param id Record id, for which execute search
     * @return Cluster, which can contain current record
     */
    protected abstract Cluster<Record> findCurrentClusterFromId(String id);

    /**
     * Add new record to data storage
     *
     * @param id     String interpretation of id
     * @param record The record being added to data storage
     * @throws DeadLockException Current record lock from other transaction
     */
    protected abstract void create(String id, Record record);

    /**
     * Add new record to data storage in current transaction
     *
     * @param id          String interpretation of id
     * @param record      The record being added to data storage
     * @param transaction Transaction, in which execute create
     * @throws DeadLockException Current record lock from other transaction
     */
    protected abstract void create(String id, Record record, Transaction transaction);

    /**
     * Add new record if record with current id not exist in data storage.
     * Update record if current id exist in data storage.
     *
     * @param record Record for save
     * @return Record with new id, if auto-generate on and record with current id not exist in data storage,
     * else return inputted record
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Record save(final Record record) {
        String id = getIdFromRecord.apply(record);
        Cluster<Record> cluster = findCurrentClusterFromId(id);
        if (Objects.nonNull(cluster) && cluster.containsKey(id)) {
            cluster.set(id, record);
        } else {
            if (needGenerateId) {
                id = generateAndSetIdForRecord(record);
            }
            create(id, record);
        }
        return record;
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
    public synchronized Record save(final Record record, final Transaction transaction) {
        String id = getIdFromRecord.apply(record);
        Cluster<Record> cluster = findCurrentClusterFromId(id);
        if (Objects.nonNull(cluster) && cluster.containsKeyFromTransaction(id)) {
            cluster.set(id, record, transaction);
        } else {
            if (needGenerateId) {
                id = generateAndSetIdForRecord(record);
            }
            create(id, record, transaction);
        }
        return record;
    }

    /**
     * Find record with current id
     *
     * @param id Id of the record being searched
     * @return Found record
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Record findById(final Object id) {
        String realId = String.valueOf(id);
        return findCurrentClusterFromId(realId).get(realId);
    }

    /**
     * Find record with current id
     *
     * @param id          Id of the record being searched
     * @param transaction Transaction, in which execute find
     * @return Found record
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Record findById(Object id, Transaction transaction) {
        String realId = String.valueOf(id);
        return findCurrentClusterFromId(realId).get(realId, transaction);
    }

    /**
     * Find all records in current repository
     *
     * @return All records, contains in current repository
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll();

    /**
     * Find all records in current repository with current transaction
     *
     * @param transaction Transaction, in which execute find
     * @return All records, contains in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Transaction transaction);

    /**
     * Find all records with pagination in this repository
     *
     * @param startIndex Quantity skipped records from start collection
     * @param rowCount   Record quantity, which need return
     * @return All record, contains in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(int startIndex, int rowCount);

    /**
     * Find all records with pagination in current repository with current transaction
     *
     * @param startIndex  Quantity skipped records from start collection
     * @param rowCount    Record quantity, which need return
     * @param transaction Transaction, in which execute find
     * @return All record, contains in current transaction in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(int startIndex, int rowCount, Transaction transaction);

    /**
     * Remove record with current id. If current cluster becomes empty it is deleted.
     *
     * @param id Id of the record being deleted
     * @return Record, which was deleted from repository, or null, if specified record not exist
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Record deleteById(final Object id) {
        String realId = String.valueOf(id);
        Cluster<Record> cluster = findCurrentClusterFromId(realId);
        if (Objects.isNull(cluster)) {
            return null;
        }
        Record record = cluster.delete(realId);
        deleteClusterIfNeed(cluster);
        return record;
    }

    /**
     * Remove record with current id in current transaction
     *
     * @param id          Id of the record being deleted
     * @param transaction Transaction, in which execute delete
     * @return Record, which was deleted from repository in current transaction, or null,
     * if specified record not exist in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Record deleteById(final Object id, final Transaction transaction) {
        String realId = String.valueOf(id);
        Cluster<Record> cluster = findCurrentClusterFromId(realId);
        if (Objects.isNull(cluster)) {
            return null;
        }
        return cluster.delete(realId, transaction);
    }

    /**
     * Remove current record
     *
     * @param record Record being deleted
     * @return Record, which was deleted, or null, where specified record not exist
     * @throws DeadLockException Current record lock from other transaction
     */
    public Record delete(final Record record) {
        return deleteById(getIdFromRecord.apply(record));
    }

    /**
     * Remove record in current transaction
     *
     * @param record Record being deleted
     * @return Record, which was deleted in current transaction, or null,
     * where specified record not exist in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    public Record delete(final Record record, final Transaction transaction) {
        return deleteById(getIdFromRecord.apply(record), transaction);
    }

    /**
     * Clear current repository from file system and RAM
     *
     * @throws DeadLockException Current record lock from other transaction
     */
    public void deleteAll() {
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            if (!file.delete())
                throw new InternalImnormException(file.getAbsolutePath() + ".delete()");
        }
    }

    /**
     * Save data from current repository to file system
     */
    public abstract void flush();

    /**
     * Checks if the cluster needs to be split and splits it if necessary
     *
     * @param cluster The cluster being checked
     */
    protected abstract void splitClusterIfNeed(Cluster<Record> cluster);

    /**
     * Check if the cluster needs to be deleted and delete it if necessary
     *
     * @param cluster The cluster being deleted
     */
    protected abstract void deleteClusterIfNeed(Cluster<Record> cluster);
    
    /**
     * Makes the repository unavailable for further use on write data
     */
    protected void lock() {
        locked = true;
    }

    /**
     * Throws an exception in case of an attempt to use a blocked repository for writing
     */
    protected void checkForBlocking() {
        if(locked)
            throw new RepositoryWasLockedException();
    }
}
