package io.github.alekseykn.imnorm;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.annotations.Id;
import io.github.alekseykn.imnorm.exceptions.CountIdException;
import io.github.alekseykn.imnorm.exceptions.CreateDataStorageException;
import io.github.alekseykn.imnorm.exceptions.IllegalGeneratedIdTypeException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public abstract class Repository<Record> {
    protected final Set<String> blockingId = ConcurrentHashMap.newKeySet();
    protected final Field recordId;
    protected final Function<Record, String> getIdFromRecord;
    protected final boolean needGenerateId;
    protected final File directory;
    protected final Gson gson = new Gson();
    protected final int sizeOfEntity;
    protected long sequence;

    protected Repository(Class<Record> type, File directory) {
        this.directory = directory;
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
                sequence = 0;
            }
        }
    }

    protected String generateAndSetIdForRecord(Record record) {
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

    protected void waitRecordForTransactions(String id) {
        try {
            while (blockingId.contains(id)) {
                synchronized (blockingId) {
                    blockingId.wait();
                }
            }
        } catch (InterruptedException ignore) {
        }
    }

    protected void waitAllRecord() {
        try {
            while (!blockingId.isEmpty()) {
                synchronized (blockingId) {
                    blockingId.wait();
                }
            }
        } catch (InterruptedException ignore) {
        }
    }

    //TODO: waitAllRecord(Transaction)

    protected void lock(String id) {
        waitRecordForTransactions(id);
        blockingId.add(id);
    }

    protected void unlock(String id) {
        blockingId.remove(id);
        synchronized (blockingId) {
            blockingId.notifyAll();
        }
    }

    protected abstract Cluster<Record> findCurrentClusterFromId(String id);

    protected abstract Record create(String id, Record record);

    public Record save(Record record) {
        String id = getIdFromRecord.apply(record);
        Cluster<Record> cluster = findCurrentClusterFromId(id);
        if (Objects.nonNull(cluster) && cluster.containsKey(id)) {
            waitRecordForTransactions(id);
            cluster.set(id, record);
            return record;
        } else {
            return create(id, record);
        }
    }

    public Record findById(Object id) {
        String realId = String.valueOf(id);
        waitRecordForTransactions(realId);
        return findCurrentClusterFromId(realId).get(realId);
    }

    public abstract Set<Record> findAll();

    public abstract Set<Record> findAll(int startIndex, int rowCount);

    public abstract Record deleteById(Object id);

    public Record delete(Record record) {
        return deleteById(getIdFromRecord.apply(record));
    }

    public abstract void flush();
}
