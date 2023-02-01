package ru.imnormproject.imnorm.repositories;

import ru.imnormproject.imnorm.annotations.Id;
import ru.imnormproject.imnorm.exceptions.CountIdException;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class Repository<Value, Key> {
    protected final Set<Value> blockingRecord = new HashSet<>();
    protected final Field recordId;
    protected final boolean needGenerateId;

    protected Repository(Class<Value> type) {
        Field[] fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> Objects.nonNull(field.getAnnotation(Id.class)))
                .toArray(Field[]::new);
        if (fields.length != 1)
            throw new CountIdException(type);
        recordId = fields[0];
        needGenerateId = recordId.getAnnotation(Id.class).autoGenerate();
    }

    public abstract Value save(Value o);

    public abstract Value find(Key id);

    public abstract Set<Value> findAll();

    public abstract Set<Value> findAll(int startIndex, int rowCount);

    public abstract Value delete(Key id);
}
