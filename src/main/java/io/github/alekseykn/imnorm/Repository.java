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

    public abstract Value save(Value o);

    public abstract Value find(Object id);

    public abstract Set<Value> findAll();

    public abstract Set<Value> findAll(int startIndex, int rowCount);

    public abstract Value delete(Object id);

    public abstract void flush();
}
