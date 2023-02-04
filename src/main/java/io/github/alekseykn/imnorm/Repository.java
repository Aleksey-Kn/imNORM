package io.github.alekseykn.imnorm;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.annotations.Id;
import io.github.alekseykn.imnorm.exceptions.CountIdException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Repository<Value> {
    protected final Set<Object> blockingId = ConcurrentHashMap.newKeySet();
    protected final Field recordId;
    protected final boolean needGenerateId;
    protected final File directory;
    protected final Gson gson = new Gson();
    protected final int sizeOfEntity;

    protected Repository(Class<Value> type, File directory) {
        this.directory = directory;

        Field[] fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> Objects.nonNull(field.getAnnotation(Id.class)))
                .toArray(Field[]::new);
        if (fields.length != 1)
            throw new CountIdException(type);
        recordId = fields[0];
        needGenerateId = recordId.getAnnotation(Id.class).autoGenerate();
        sizeOfEntity = type.getDeclaredFields().length * 50;
    }

    protected void waitRecordForTransactions(Object id) {
        int time = 10;
        try {
            while (blockingId.contains(id)) {
                Thread.sleep(time);
                time *= 2;
            }
        } catch (InterruptedException ignored) {
        }
    }

    protected void waitAllRecord() {
        int time = 10;
        try {
            while (blockingId.isEmpty()) {
                Thread.sleep(time);
                time *= 2;
            }
        } catch (InterruptedException ignored) {
        }
    }

    protected abstract Cluster<Value> findCurrentClusterFromId(Object id);

    protected abstract Value create(Object id, Value record);

    public Value save(Value record) {
        try {
            Object id = recordId.get(record);
            Cluster<Value> cluster = findCurrentClusterFromId(id);
            if (Objects.nonNull(cluster) && cluster.containsKey(id)) {
                waitRecordForTransactions(id);
                cluster.set(id, record);
                return record;
            } else {
                return create(id, record);
            }
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }

    public Value findById(Object id) {
        waitRecordForTransactions(id);
        return findCurrentClusterFromId(id).get(id);
    }

    public abstract Set<Value> findAll();

    public abstract Set<Value> findAll(int startIndex, int rowCount);

    public abstract Value deleteById(Object id);

    public Value delete(Value record) {
        try {
            return deleteById(recordId.get(record));
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }

    public abstract void flush();
}
