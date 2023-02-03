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
import java.util.function.Function;

public abstract class Repository<Value> {
    protected final Set<String> blockingId = ConcurrentHashMap.newKeySet();
    protected final Function<Value, String> getStringIdFromRecord;
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
        getStringIdFromRecord = record -> {
            try {
                return String.valueOf(fields[0].get(record));
            } catch (IllegalAccessException e) {
                throw new InternalImnormException(e.getMessage());
            }
        };
        needGenerateId = fields[0].getAnnotation(Id.class).autoGenerate();
        sizeOfEntity = type.getDeclaredFields().length * 50;
    }

    protected void waitRecordForTransactions(String id) {
        int time = 10;
        try {
            while (blockingId.contains(id)) {
                Thread.sleep(time);
                time *= 2;
            }
        } catch (InterruptedException ignored) {}
    }

    protected abstract Cluster<Value> findCurrentCluster(Object id);

    protected abstract Value create(Object id, Value record);

    public Value save(Value record) {
        String id = getStringIdFromRecord.apply(record);
        Cluster<Value> cluster = findCurrentCluster(id);
        if (Objects.nonNull(cluster) && cluster.containsKey(id)) {
            waitRecordForTransactions(id);
            cluster.set(id, record);
            return record;
        } else {
            return create(id, record);
        }
    }

    public Value findById(String id) {
        waitRecordForTransactions(id);
        return findCurrentCluster(id).get(id);
    }

    public abstract Set<Value> findAll();

    public abstract Set<Value> findAll(int startIndex, int rowCount);

    public abstract Value deleteById(String id);

    public Value delete(Value record) {
        return deleteById(getStringIdFromRecord.apply(record));
    }

    public abstract void flush();
}
