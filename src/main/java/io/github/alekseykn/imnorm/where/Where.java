package io.github.alekseykn.imnorm.where;

import io.github.alekseykn.imnorm.exceptions.IllegalFieldNameException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.function.Predicate;

public final class Where<T> implements Condition{
    private final String fieldName;
    private final Predicate<T> condition;

    public Where(final String fieldName, final Predicate<T> predicate) {
        this.fieldName = fieldName;
        condition = predicate;
    }

    public Where(final String fieldName, final CompareMode compareMode, final Comparable<T> origin) {
        this.fieldName = fieldName;
        condition =  field -> compareMode.checkCondition(origin, field);
    }

    public Where(final String fieldName, final CompareMode compareMode, final T origin, final Comparator<T> comparator) {
        this.fieldName = fieldName;
        condition = field -> compareMode.checkCondition(origin, field, comparator);
    }

    public ConditionSet and(final Where<?> where) {
        return ConditionSet.and(this, where);
    }

    public ConditionSet or(final Where<?> where) {
        return ConditionSet.or(this, where);
    }

    @Override
    public boolean fitsCondition(final Object record) {
        try {
            Field field = record.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return condition.test((T) field.get(record));
        } catch (NoSuchFieldException e) {
            throw new IllegalFieldNameException(fieldName, record.getClass());
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }
}

