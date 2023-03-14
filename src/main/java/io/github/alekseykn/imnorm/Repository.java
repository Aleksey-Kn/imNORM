package io.github.alekseykn.imnorm;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.annotations.Id;
import io.github.alekseykn.imnorm.exceptions.CountIdException;
import io.github.alekseykn.imnorm.exceptions.CreateDataStorageException;
import io.github.alekseykn.imnorm.exceptions.IllegalGeneratedIdTypeException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;
import io.github.alekseykn.imnorm.exceptions.RepositoryWasLockedException;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;

import io.github.alekseykn.imnorm.exceptions.DeadLockException;

import java.util.function.Function;
import java.util.stream.Collectors;

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
     * Determines whether to generate an identifier for the current record
     *
     * @param record The record being checked
     * @return True, if the current record needs to generate an ID
     */
    private boolean needGenerateIdForRecord(final Record record) {
        try {
            return needGenerateId
                    && switch (recordId.getType().getSimpleName().toLowerCase(Locale.ROOT)) {
                case "byte" -> recordId.get(record).equals((byte) 0);
                case "short" -> recordId.get(record).equals((short) 0);
                case "int" -> recordId.get(record).equals(0);
                case "long" -> recordId.get(record).equals(0L);
                case "float" -> recordId.get(record).equals(0f);
                case "double" -> recordId.get(record).equals(0d);
                case "string" -> recordId.get(record).toString().isEmpty();
                default -> throw new IllegalGeneratedIdTypeException();
            };
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }

    /**
     * Create and set id needed type for current record
     *
     * @param record Record, which needed create id
     * @return Created id
     */
    private String generateAndSetIdForRecord(final Record record) {
        try {
            switch (recordId.getType().getSimpleName().toLowerCase(Locale.ROOT)) {
                case "byte" -> recordId.set(record, (byte) sequence);
                case "short" -> recordId.set(record, (short) sequence);
                case "int" -> recordId.set(record, (int) sequence);
                case "long" -> recordId.set(record, sequence);
                case "float" -> recordId.set(record, (float) sequence);
                case "double" -> recordId.set(record, (double) sequence);
                default -> recordId.set(record, Long.toString(sequence));
            }
            return Long.toString(sequence++);
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
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
     * Add new cluster and insert current record
     *
     * @param id     String interpretation of id
     * @param record The record being added to data storage
     */
    protected abstract void createClusterForRecord(String id, Record record);

    /**
     * Add new cluster and insert current record in current transaction
     *
     * @param id          String interpretation of id
     * @param record      The record being added to data storage
     * @param transaction Transaction, in which execute create
     */
    protected abstract void createClusterForRecord(String id, Record record, Transaction transaction);

    /**
     * Add new record if record with current id not exist in data storage.
     * Update record if current id exist in data storage.
     * If cluster too large, split it on two cluster.
     *
     * @param record Record for save
     * @return Record with new id, if auto-generate on and record with current id not exist in data storage,
     * else return inputted record
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Record save(final Record record) {
        checkForBlocking();
        String id = needGenerateIdForRecord(record) ? generateAndSetIdForRecord(record) : getIdFromRecord.apply(record);
        Cluster<Record> cluster = findCurrentClusterFromId(id);

        if (Objects.nonNull(cluster)) {
            if (cluster.containsKey(id)) {
                cluster.set(id, record);
            } else {
                cluster.set(id, record);
                splitClusterIfNeed(cluster);
            }
        } else {
            createClusterForRecord(id, record);
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
        checkForBlocking();
        String id = needGenerateIdForRecord(record) ? generateAndSetIdForRecord(record) : getIdFromRecord.apply(record);
        Cluster<Record> cluster = findCurrentClusterFromId(id);

        if (Objects.nonNull(cluster)) {
            cluster.set(id, record, transaction);
        } else {
            createClusterForRecord(id, record, transaction);
        }

        return record;
    }

    /**
     * Create new cluster from current record list
     *
     * @param records Records for insert to cluster
     * @return Cluster, which contains current records list
     */
    protected Cluster<Record> createClusterFromList(final List<Record> records) {
        TreeMap<String, Record> inputData = new TreeMap<>();
        for (Record record : records) {
            inputData.put(getIdFromRecord.apply(record), record);
        }
        return new Cluster<>(inputData, this);
    }

    /**
     * Create new cluster from current record list in current transaction
     *
     * @param records     Records for insert to cluster
     * @param transaction Transaction, in which execute create
     * @return Cluster, which contains current records list
     */
    protected Cluster<Record> createClusterFromList(final List<Record> records, final Transaction transaction) {
        TreeMap<String, Record> inputData = new TreeMap<>();
        for (Record record : records) {
            inputData.put(getIdFromRecord.apply(record), record);
        }
        return new Cluster<>(inputData, this, transaction);
    }

    /**
     * Add new cluster and insert current collection in it
     *
     * @param records Records, for which needed to create new cluster
     */
    protected abstract void createClusterForRecords(List<Record> records);

    /**
     * Add new cluster and insert current collection in current transaction
     *
     * @param records     Records, for which needed to create new cluster
     * @param transaction Transaction, in which execute create
     */
    protected abstract void createClusterForRecords(List<Record> records, Transaction transaction);

    /**
     * Make sorted by id list and change record id, where necessary
     *
     * @param records Records collection
     * @return Sorted by id records list
     */
    private List<Record> createRecordsSortedList(final Collection<Record> records) {
        return records.stream()
                .peek(record -> {
                    if (needGenerateIdForRecord(record))
                        generateAndSetIdForRecord(record);
                }).sorted(Comparator.comparing(getIdFromRecord).reversed())
                .collect(Collectors.toList());
    }

    /**
     * Save records collection to data storage: add new records and update exists records.
     * All too large clusters split.
     *
     * @param records Added records collection
     * @return Incoming collection with changed ids, where necessary
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Set<Record> saveAll(final Collection<Record> records) {
        if (records.isEmpty())
            return null;
        List<Record> sortedRecords = createRecordsSortedList(records);

        Cluster<Record> cluster = findCurrentClusterFromId(getIdFromRecord.apply(sortedRecords.get(0)));
        if(Objects.isNull(cluster)) {
            createClusterForRecords(sortedRecords);
        } else {
            String id;
            for (Record record : records) {
                id = getIdFromRecord.apply(record);
                if (id.compareTo(cluster.getFirstKey()) < 0) {
                    splitClusterIfNeed(cluster);
                    cluster = findCurrentClusterFromId(id);
                    if (Objects.isNull(cluster)) {
                        createClusterForRecords(sortedRecords.subList(sortedRecords.indexOf(record),
                                sortedRecords.size()));
                        break;
                    }
                }
                cluster.set(id, record);
            }
        }

        return new HashSet<>(sortedRecords);
    }

    /**
     * Save records collection to data storage: add new records and update exists records.
     * All too large clusters split.
     *
     * @param records Added records collection
     * @param transaction Transaction, in which execute save
     * @return Incoming collection with changed ids, where necessary
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Set<Record> saveAll(final Collection<Record> records, Transaction transaction) {
        if (records.isEmpty())
            return null;
        List<Record> sortedRecords = createRecordsSortedList(records);

        Cluster<Record> cluster = findCurrentClusterFromId(getIdFromRecord.apply(sortedRecords.get(0)));
        if(Objects.isNull(cluster)) {
            createClusterForRecords(sortedRecords);
        } else {
            String id;
            for (Record record : records) {
                id = getIdFromRecord.apply(record);
                if (id.compareTo(cluster.getFirstKey()) < 0) {
                    cluster = findCurrentClusterFromId(id);
                    if (Objects.isNull(cluster)) {
                        createClusterForRecords(sortedRecords.subList(sortedRecords.indexOf(record),
                                sortedRecords.size()), transaction);
                        break;
                    }
                }
                cluster.set(id, record, transaction);
            }
        }

        return new HashSet<>(sortedRecords);
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
     * @return All records, contains in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(int startIndex, int rowCount);

    /**
     * Find all records with pagination in current repository with current transaction
     *
     * @param startIndex  Quantity skipped records from start collection
     * @param rowCount    Record quantity, which need return
     * @param transaction Transaction, in which execute find
     * @return All records, contains in current transaction in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(int startIndex, int rowCount, Transaction transaction);

    /**
     * Find all records in current repository, suitable for the specified condition
     *
     * @return All records, suitable for the specified condition
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Condition<Record> condition);

    /**
     * Find all records, suitable for the specified condition in current transaction
     *
     * @param transaction Transaction, in which execute find
     * @return Suitable for the specified condition records, contains in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Condition<Record> condition, Transaction transaction);

    /**
     * Find all records with pagination, suitable for the specified condition
     *
     * @param startIndex Quantity skipped records from start collection
     * @param rowCount   Record quantity, which need return
     * @return Suitable for the specified condition records, contains in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Condition<Record> condition, int startIndex, int rowCount);

    /**
     * Find all records with pagination, suitable for the specified condition in current transaction
     *
     * @param startIndex  Quantity skipped records from start collection
     * @param rowCount    Record quantity, which need return
     * @param transaction Transaction, in which execute find
     * @return Suitable for the specified condition records, contains in current transaction in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Condition<Record> condition, int startIndex, int rowCount, Transaction transaction);

    /**
     * Remove record with current id. If current cluster becomes empty it is deleted.
     *
     * @param id Id of the record being deleted
     * @return Record, which was deleted from repository, or null, if specified record not exist
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Record deleteById(final Object id) {
        checkForBlocking();
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
        checkForBlocking();
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
    public synchronized void deleteAll() {
        checkForBlocking();
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            if (!file.delete())
                throw new InternalImnormException(file.getAbsolutePath() + ".delete()");
        }
    }

    /**
     * Save data from current repository to file system
     */
    public void flush(){
        if (needGenerateId) {
            try (DataOutputStream outputStream = new DataOutputStream(
                    new FileOutputStream(new File(directory.getAbsolutePath(), "_sequence.imnorm")))) {
                outputStream.writeLong(sequence);
            } catch (IOException e) {
                throw new InternalImnormException(e);
            }
        }
    }

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
        if (locked)
            throw new RepositoryWasLockedException();
    }
}
