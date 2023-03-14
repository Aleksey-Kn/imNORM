package io.github.alekseykn.imnorm.where;

import io.github.alekseykn.imnorm.exceptions.IllegalFieldNameException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.function.Predicate;

/**
 * Atomic condition for comparing entity field
 *
 * @param <F> Field type
 * @param <E> Entity type
 */
public final class Where<F, E> implements Condition<E> {
    /**
     * Name of the entity field for comparison
     */
    private final String fieldName;

    /**
     * Condition for comparison
     */
    private final Predicate<F> condition;

    /**
     * Set input predicate as condition for comparison
     *
     * @param fieldName Name of the entity field for comparison
     * @param predicate Condition for comparison
     */
    public Where(final String fieldName, final Predicate<F> predicate) {
        this.fieldName = fieldName;
        condition = predicate;
    }

    /**
     * Create condition for comparison from input compare mode
     *
     * @param fieldName   Name of the entity field for comparison
     * @param compareMode Operation for comparison
     * @param origin      The operand with which the entity field will be compared
     */
    public Where(final String fieldName, final CompareMode compareMode, final Comparable<F> origin) {
        this.fieldName = fieldName;
        condition = field -> compareMode.checkCondition(origin, field);
    }

    /**
     * Create condition for comparison from input compare mode and input comparison rules
     *
     * @param fieldName   Name of the entity field for comparison
     * @param compareMode Operation for comparison
     * @param origin      The operand with which the entity field will be compared
     * @param comparator  Comparison rules
     */
    public Where(final String fieldName, final CompareMode compareMode, final F origin, final Comparator<F> comparator) {
        this.fieldName = fieldName;
        condition = field -> compareMode.checkCondition(origin, field, comparator);
    }

    /**
     * Combines conditions using a logical 'and'
     *
     * @param where Condition for combine
     * @return A set of conditions for comparison
     */
    public ConditionSet<E> and(final Where<?, E> where) {
        return ConditionSet.and(this, where);
    }

    /**
     * Combines conditions using a logical 'or'
     *
     * @param where Condition for combine
     * @return A set of conditions for comparison
     */
    public ConditionSet<E> or(final Where<?, E> where) {
        return ConditionSet.or(this, where);
    }

    /**
     * Checks whether the passed record meets the condition
     *
     * @param record The record being checked
     * @return True, if the record fits the condition
     * @throws IllegalFieldNameException Specified field not exists in current entity
     * @throws InternalImnormException   Reflection error
     * @throws ClassCastException        Condition type does not match field type
     */
    @Override
    public boolean fitsCondition(final E record) {
        try {
            Field field = record.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return condition.test((F) field.get(record));
        } catch (NoSuchFieldException e) {
            throw new IllegalFieldNameException(fieldName, record.getClass());
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }
}

