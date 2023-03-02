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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public abstract class Repository<Record> {
    protected static final int CLUSTER_MAX_SIZE = 10_000;

    protected final Field recordId;
    protected final Function<Record, String> getIdFromRecord;
    protected final boolean needGenerateId;
    protected final File directory;
    protected final Gson gson = new Gson();
    protected final int sizeOfEntity;
    protected long sequence;
    protected Class<Record> type;

    protected Repository(Class<Record> type, File directory) {
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

    protected abstract Cluster<Record> findCurrentClusterFromId(String id);

    protected abstract Record create(String id, Record record);

    protected abstract Record create(String id, Record record, Transaction transaction);

    public Record save(Record record) {
        String id = getIdFromRecord.apply(record);
        Cluster<Record> cluster = findCurrentClusterFromId(id);
        if (Objects.nonNull(cluster) && cluster.containsKey(id)) {
            synchronized (this) {
                cluster.set(id, record);
            }
            return record;
        } else {
            if(needGenerateId) {
                id = generateAndSetIdForRecord(record);
            }
            return create(id, record);
        }
    }

    public Record save(Record record, Transaction transaction) {
        String id = getIdFromRecord.apply(record);
        Cluster<Record> cluster = findCurrentClusterFromId(id);
        if (Objects.nonNull(cluster) && cluster.containsKey(id, transaction)) {
            synchronized (this) {
                cluster.set(id, record, transaction);
            }
            return record;
        } else {
            if(needGenerateId) {
                id = generateAndSetIdForRecord(record);
            }
            return create(id, record, transaction);
        }
    }

    public synchronized Record findById(Object id) {
        String realId = String.valueOf(id);
        return findCurrentClusterFromId(realId).get(realId);
    }

    public synchronized Record findById(Object id, Transaction transaction) {
        String realId = String.valueOf(id);
        return findCurrentClusterFromId(realId).get(realId, transaction);
    }

    public abstract Set<Record> findAll();

    public abstract Set<Record> findAll(Transaction transaction);

    public abstract Set<Record> findAll(int startIndex, int rowCount);

    public abstract Set<Record> findAll(int startIndex, int rowCount, Transaction transaction);

    public abstract Record deleteById(Object id);

    public abstract Record deleteById(Object id, Transaction transaction);

    public Record delete(Record record) {
        return deleteById(getIdFromRecord.apply(record));
    }

    public Record delete(Record record, Transaction transaction) {
        return deleteById(getIdFromRecord.apply(record));
    }

    public void deleteAll() {
        for(File file: Objects.requireNonNull(directory.listFiles())) {
            if(!file.delete())
                throw new InternalImnormException(file.getAbsolutePath() + ".delete()");
        }
    };

    public abstract void flush();
}
