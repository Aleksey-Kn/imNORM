package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.annotations.GeneratedValue;
import io.github.alekseykn.imnorm.exceptions.*;
import io.github.alekseykn.imnorm.utils.ClusterFileManipulator;
import io.github.alekseykn.imnorm.utils.FieldUtil;
import io.github.alekseykn.imnorm.where.Condition;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
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
    protected static final int CLUSTER_MAX_SIZE = 100_000;

    /**
     * Entity id field
     */
    protected final Field recordId;

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
    protected final ClusterFileManipulator<Record> clusterFileManipulator;

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

        recordId = FieldUtil.getIdField(type);
        needGenerateId = recordId.getAnnotation(GeneratedValue.class) != null;
        sizeOfEntity = FieldUtil.countFields(type) * 50;
        clusterFileManipulator = new ClusterFileManipulator<>(type, recordId);

        if (needGenerateId) {
            try (DataInputStream fileInputStream = new DataInputStream(
                    new FileInputStream(new File(directory.getAbsolutePath(), "_sequence.imnorm")))) {
                sequence = Math.max(fileInputStream.readLong(), recordId.getAnnotation(GeneratedValue.class).startId());
            } catch (IOException e) {
                sequence = recordId.getAnnotation(GeneratedValue.class).startId();
            }
        }
    }

    /**
     * Function for get hash id entity id as int
     */
    protected int getHashIdFromRecord(final Record record) {
        try {
            return getHashFromId(recordId.get(record));
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }

    /**
     * Function for get data entity id as string
     */
    protected Object getOriginalIdFromRecord(final Record record) {
        try {
            return recordId.get(record);
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }

    /**
     * Function for get string hash of id
     */
    protected int getHashFromId(final Object id) {
        if (id instanceof Integer || id instanceof Byte || id instanceof Short || id instanceof Character) {
            return (int) id;
        } else {
            return id.hashCode();
        }
    }

    /**
     * Create and set id needed type for current record if needed
     *
     * @param record Record, which needed to create id
     */
    protected void generateAndSetIdForRecordIfNeeded(final Record record) {
        if (needGenerateId) {
            try {
                switch (recordId.getType().getSimpleName().toLowerCase(Locale.ROOT)) {
                    case "byte" -> {
                        if (recordId.get(record).equals((byte) 0))
                            recordId.set(record, (byte) sequence++);
                    }
                    case "short" -> {
                        if (recordId.get(record).equals((short) 0))
                            recordId.set(record, (short) sequence++);
                    }
                    case "int" -> {
                        if (recordId.get(record).equals(0))
                            recordId.set(record, (int) sequence++);
                    }
                    case "long" -> {
                        if (recordId.get(record).equals(0L))
                            recordId.set(record, sequence++);
                    }
                    case "float" -> {
                        if (recordId.get(record).equals(0f))
                            recordId.set(record, (float) sequence++);
                    }
                    case "double" -> {
                        if (recordId.get(record).equals(0d))
                            recordId.set(record, (double) sequence++);
                    }
                    case "string" -> {
                        if (recordId.get(record).toString().isEmpty())
                            recordId.set(record, Long.toString(sequence++));
                    }
                    default -> throw new IllegalGeneratedIdTypeException();
                }
            } catch (IllegalAccessException e) {
                throw new InternalImnormException(e);
            }
        }
    }

    /**
     * Determines whether to split the current cluster based on its size
     * and the total number of clusters in the repository.
     * It is necessary in order to control the number of files for clusters
     * and to prevent an increased load on the file system due to too many of them.
     *
     * @param clusterQuantity Total number of clusters in the repository
     * @param currentClusterSize Number of records in the current cluster
     * @return Decision on the need to split this cluster
     */
    protected boolean needSplit(final int clusterQuantity, final int currentClusterSize) {
        return clusterQuantity < 1000 && currentClusterSize * sizeOfEntity > CLUSTER_MAX_SIZE
                || clusterQuantity > 1000
                && clusterQuantity * sizeOfEntity > CLUSTER_MAX_SIZE * Math.pow((float) currentClusterSize / 1000, 2);
    }

    /**
     * Find cluster, which can contain record with current id
     *
     * @param id Record id, for which execute search
     * @return Cluster, which can contain current record
     */
    protected abstract Optional<Cluster<Record>> findCurrentClusterFromId(int id);

    /**
     * Add new cluster and insert current record
     *
     * @param id     String interpretation of id
     * @param record The record being added to data storage
     */
    protected abstract void createClusterForRecord(int hashId, Object id, Record record);

    /**
     * Add new cluster and insert current record in current transaction
     *
     * @param id          String interpretation of id
     * @param record      The record being added to data storage
     * @param transaction Transaction, in which execute create
     */
    protected abstract void createClusterForRecord(int hashId, Object id, Record record, Transaction transaction);

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
        generateAndSetIdForRecordIfNeeded(record);
        int hashId = getHashIdFromRecord(record);
        Object id = getOriginalIdFromRecord(record);

        findCurrentClusterFromId(hashId).ifPresentOrElse(cluster -> {
            cluster.set(hashId, id, record);
            splitClusterIfNeed(cluster);
        }, () -> createClusterForRecord(hashId, id, record));

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
        generateAndSetIdForRecordIfNeeded(record);
        int hashId = getHashIdFromRecord(record);
        Object id = getOriginalIdFromRecord(record);

        findCurrentClusterFromId(hashId).ifPresentOrElse(cluster -> cluster.set(hashId, id, record, transaction),
                () -> createClusterForRecord(hashId, id, record, transaction));

        return record;
    }

    /**
     * Create record map from record list
     *
     * @param records Record list
     * @return Map, with contains hash, id and record
     */
    private TreeMap<Integer, Map<Object, Record>> convertListToMap(final List<Record> records) {
        TreeMap<Integer, Map<Object, Record>> data = new TreeMap<>();
        int hash;
        Object id;

        for (Record record : records) {
            id = getOriginalIdFromRecord(record);
            hash = getHashFromId(id);
            if (!data.containsKey(hash)) {
                data.put(hash, new HashMap<>());
            }
            data.get(hash).put(id, record);
        }

        return data;
    }

    /**
     * Add new cluster and insert current collection in it
     *
     * @param records Records, for which needed to create new cluster
     */
    protected Cluster<Record> createClusterForRecords(final List<Record> records) {
        TreeMap<Integer, Map<Object, Record>> data = convertListToMap(records);
        return new Cluster<>(data.firstKey(), data, this);
    }

    /**
     * Add new cluster and insert current collection in current transaction
     *
     * @param records     Records, for which needed to create new cluster
     * @param transaction Transaction, in which execute create
     */
    protected Cluster<Record> createClusterForRecords(final List<Record> records, final Transaction transaction) {
        TreeMap<Integer, Map<Object, Record>> data = convertListToMap(records);
        return new Cluster<>(data.firstKey(), data, this, transaction);
    }

    /**
     * Make sorted by id list and change record id, where necessary
     *
     * @param records Records collection
     * @return Sorted by id records list
     */
    private List<Record> createRecordsSortedList(final Collection<Record> records) {
        return records.stream()
                .peek(this::generateAndSetIdForRecordIfNeeded)
                .sorted(Comparator.comparing(this::getHashIdFromRecord).reversed())
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
            return Set.of();
        List<Record> sortedRecords = createRecordsSortedList(records);

        Optional<Cluster<Record>> cluster = findCurrentClusterFromId(getHashIdFromRecord(sortedRecords.get(0)));
        if (cluster.isEmpty()) {
            createClusterForRecords(sortedRecords);
        } else {
            int id;
            for (Record record : records) {
                id = getHashIdFromRecord(record);
                if (id < cluster.get().getFirstKey()) {
                    splitClusterIfNeed(cluster.get());
                    cluster = findCurrentClusterFromId(id);
                    if (cluster.isEmpty()) {
                        createClusterForRecords(sortedRecords.subList(sortedRecords.indexOf(record),
                                sortedRecords.size()));
                        return new HashSet<>(sortedRecords);
                    }
                }
                cluster.get().set(id, getOriginalIdFromRecord(record), record);
            }
            splitClusterIfNeed(cluster.get());
        }

        return new HashSet<>(sortedRecords);
    }

    /**
     * Save records collection to data storage: add new records and update exists records.
     * All too large clusters split.
     *
     * @param records     Added records collection
     * @param transaction Transaction, in which execute save
     * @return Incoming collection with changed ids, where necessary
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Set<Record> saveAll(final Collection<Record> records, final Transaction transaction) {
        if (records.isEmpty())
            return Set.of();
        List<Record> sortedRecords = createRecordsSortedList(records);

        Optional<Cluster<Record>> cluster = findCurrentClusterFromId(getHashIdFromRecord(sortedRecords.get(0)));
        if (cluster.isEmpty()) {
            createClusterForRecords(sortedRecords);
        } else {
            int id;
            for (Record record : records) {
                id = getHashIdFromRecord(record);
                if (id < cluster.get().getFirstKey()) {
                    cluster = findCurrentClusterFromId(id);
                    if (cluster.isEmpty()) {
                        createClusterForRecords(sortedRecords.subList(sortedRecords.indexOf(record),
                                sortedRecords.size()), transaction);
                        return new HashSet<>(sortedRecords);
                    }
                }
                cluster.get().set(id, getOriginalIdFromRecord(record), record, transaction);
            }
            splitClusterIfNeed(cluster.get());
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
    public Optional<Record> findById(final Object id) {
        int hash = getHashFromId(id);
        return Optional.ofNullable(findCurrentClusterFromId(hash).orElseThrow().get(hash, id));
    }

    /**
     * Find record with current id
     *
     * @param id          Id of the record being searched
     * @param transaction Transaction, in which execute find
     * @return Found record
     * @throws DeadLockException Current record lock from other transaction
     */
    public Optional<Record> findById(final Object id, final Transaction transaction) {
        int hash = getHashFromId(id);
        return Optional.ofNullable(findCurrentClusterFromId(hash).orElseThrow().get(hash, id, transaction));
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
     * @param condition Condition for search
     * @return All records, suitable for the specified condition
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Condition<Record> condition);

    /**
     * Find all records, suitable for the specified condition in current transaction
     *
     * @param condition   Condition for search
     * @param transaction Transaction, in which execute find
     * @return Suitable for the specified condition records, contains in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Condition<Record> condition, Transaction transaction);

    /**
     * Find all records with pagination, suitable for the specified condition
     *
     * @param condition  Condition for search
     * @param startIndex Quantity skipped records from start collection
     * @param rowCount   Record quantity, which need return
     * @return Suitable for the specified condition records, contains in current diapason
     * @throws DeadLockException Current record lock from other transaction
     */
    public abstract Set<Record> findAll(Condition<Record> condition, int startIndex, int rowCount);

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
    public abstract Set<Record> findAll(Condition<Record> condition, int startIndex, int rowCount, Transaction transaction);

    /**
     * Remove record with current id. If current cluster becomes empty it is deleted.
     *
     * @param id Id of the record being deleted
     * @return Record, which was deleted from repository, or null, if specified record not exist
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Optional<Record> deleteById(final Object id) {
        return innerDelete(getHashFromId(id), id);
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
    public synchronized Optional<Record> deleteById(final Object id, final Transaction transaction) {
        return innerDelete(getHashFromId(id), id, transaction);
    }

    /**
     * Remove current record
     *
     * @param record Record being deleted
     * @return Record, which was deleted, or null, where specified record not exist
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Optional<Record> delete(final Record record) {
        return innerDelete(getHashIdFromRecord(record), getOriginalIdFromRecord(record));
    }

    /**
     * Remove record in current transaction
     *
     * @param record Record being deleted
     * @return Record, which was deleted in current transaction, or null,
     * where specified record not exist in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    public synchronized Optional<Record> delete(final Record record, final Transaction transaction) {
        return innerDelete(getHashIdFromRecord(record), getOriginalIdFromRecord(record), transaction);
    }

    /**
     * Remove record with specified id hash. If current cluster becomes empty it is deleted.
     *
     * @param hash Hash of id of the record being deleted
     * @param id   ID of the record being deleted
     * @return Record, which was deleted from repository in current transaction, or null,
     * if specified record not exist in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    protected synchronized Optional<Record> innerDelete(final int hash, final Object id) {
        checkForBlocking();
        Optional<Cluster<Record>> cluster = findCurrentClusterFromId(hash);
        if (cluster.isEmpty()) {
            return Optional.empty();
        } else {
            Record record = cluster.get().delete(hash, id);
            deleteClusterIfNeed(cluster.get());
            return Optional.ofNullable(record);
        }
    }

    /**
     * Remove record with specified id hash in current transaction
     *
     * @param hash        Hash of id of the record being deleted
     * @param id          ID of the record being deleted
     * @param transaction Transaction, in which execute delete
     * @return Record, which was deleted from repository in current transaction, or null,
     * if specified record not exist in current transaction
     * @throws DeadLockException Current record lock from other transaction
     */
    protected synchronized Optional<Record> innerDelete(final int hash, final Object id, final Transaction transaction) {
        checkForBlocking();
        Optional<Cluster<Record>> cluster = findCurrentClusterFromId(hash);
        if (cluster.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(cluster.get().delete(hash, id, transaction));
        }
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
    public synchronized void flush() {
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
     * @return Number of records in the repository
     */
    public abstract long size();

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

    /**
     * Checks the existence of a record with the specified id
     *
     * @param id The id being checked
     * @return True, if record is exist
     */
    protected abstract boolean existsById(Object id);
}
