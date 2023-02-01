package ru.imnormproject.imnorm.repositories;

import ru.imnormproject.imnorm.annotations.Id;
import ru.imnormproject.imnorm.exceptions.CountIdException;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class Repository {
    protected final Set<Object> blockingRecord = new HashSet<>();
    protected final Field recordId;
    protected final boolean needGenerateId;

    protected Repository(Class type) {
        Field[] fields = Arrays.stream(type.getDeclaredFields())
                .filter(field -> Objects.nonNull(field.getAnnotation(Id.class)))
                .toArray(Field[]::new);
        if(fields.length != 1)
            throw new CountIdException(type);
        recordId = fields[0];
        needGenerateId = recordId.getAnnotation(Id.class).autoGenerate();
    }
}
