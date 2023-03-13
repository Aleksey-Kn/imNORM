package io.github.alekseykn.imnorm.where;

import lombok.AccessLevel;
import lombok.Getter;
import io.github.alekseykn.imnorm.IllegalFieldNameException;
import io.github.alekseykn.imnorm.InternalImnormException;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Predicate;

public class Where<T> {
    @Getter(AccessLevel.PUBLIC)
    private final String fieldName;
    private final Predicate<T> condition;
    private Where<?> nextCondition = null;
    private boolean and;

    public Where(final String fieldName, final Predicate<T> predicate) {
        this.fieldName = fieldName;
        condition = predicate;
    }

    public Where(final String fieldName, final CompareMode compareMode, final Comparable<T> origin) {
        this.fieldName = fieldName;
        condition =  field -> compareMode.checkCondition(origin, field);
    }

    public Where(final String fieldName, final CompareMode compareMode, final T origin, Comparator<T> comparator) {
        this.fieldName = fieldName;
        condition = field -> compareMode.checkCondition(origin, field, comparator);
    }

    public Where<?> and(final Where<?> where) {
        where.and = true;
        where.nextCondition = this;
        return where;
    }

    public Where<?> or(final Where<?> where) {
        where.and = false;
        where.nextCondition = this;
        return where;
    }

    public boolean fitsCondition(final Object record) {
        try {
            Field field = record.getClass().getField(fieldName);
            field.setAccessible(true);
            boolean resultCurrentWhere = condition.test((T) field.get(record));
            if (Objects.isNull(nextCondition)) {
                return resultCurrentWhere;
            } else {
                return and ? resultCurrentWhere && nextCondition.fitsCondition(record)
                        : resultCurrentWhere || nextCondition.fitsCondition(record);
            }
        } catch (NoSuchFieldException e) {
            throw new IllegalFieldNameException(fieldName, record.getClass());
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }
}
