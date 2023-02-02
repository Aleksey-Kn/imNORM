package io.github.alekseykn.imnorm;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.annotations.Id;
import io.github.alekseykn.imnorm.exceptions.CountIdException;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class Repository<Value> {
    protected final Set<Value> blockingRecord = new HashSet<>();
    protected final Field recordId;
    protected final boolean needGenerateId;
    protected final File directory;
    protected final Gson gson = new Gson();

    protected Repository(Class<Value> type, File directory) {
        this.directory = directory;

        Field[] fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> Objects.nonNull(field.getAnnotation(Id.class)))
                .toArray(Field[]::new);
        if (fields.length != 1)
            throw new CountIdException(type);
        recordId = fields[0];
        needGenerateId = recordId.getAnnotation(Id.class).autoGenerate();
    }

    protected abstract Cluster<Value> findCurrentCluster(Object recordInCluster);

    protected abstract Value create(Object id, Value record);

    public Value save(Value record) {
        try {
            Cluster<Value> cluster = findCurrentCluster(record);
            Object key = recordId.get(record);
            if (cluster.containsKey(key)) {
                cluster.set(key, record);
                return record;
            } else {
                return create(key, record);
            }
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e.getMessage());
        }
    }

    public Value findById(Object id) {
        return findCurrentCluster(id).get(id);
    }

    public abstract Set<Value> findAll();

    public abstract Set<Value> findAll(int startIndex, int rowCount);

    public Value deleteById(Object id) {
        return findCurrentCluster(id).delete(id);
    }

    public Value delete(Value record) {
        try {
            return deleteById(recordId.get(record));
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e.getMessage());
        }
    }

    public abstract void flush();
}
