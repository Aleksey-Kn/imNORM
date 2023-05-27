package io.github.alekseykn.imnorm.where;

import io.github.alekseykn.imnorm.exceptions.IllegalFieldNameException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;
import io.github.alekseykn.imnorm.utils.FieldUtil;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.function.Predicate;

/**
 * Atomic condition for comparing entity field. Required for analyzing entity fields that do not have getters.
 *
 * @param <Fld> Field type
 * @param <Entity> Entity type
 */
public final class FieldCondition<Fld, Entity> implements Condition<Entity> {
    /**
     * Name of the entity field for comparison
     */
    private final String fieldName;

    /**
     * Condition for comparison
     */
    private final Predicate<Fld> condition;

    /**
     * Set input predicate as condition for comparison
     *
     * @param fieldName Name of the entity field for comparison
     * @param predicate Condition for comparison
     */
    public FieldCondition(final String fieldName, final Predicate<Fld> predicate) {
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
    public FieldCondition(final String fieldName, final CompareMode compareMode, final Comparable<Fld> origin) {
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
    public FieldCondition(final String fieldName, final CompareMode compareMode, final Fld origin, final Comparator<Fld> comparator) {
        this.fieldName = fieldName;
        condition = field -> compareMode.checkCondition(origin, field, comparator);
    }

    /**
     * Combines conditions using a logical 'and'
     *
     * @param fieldCondition Condition for combine
     * @return A set of conditions for comparison
     */
    public ConditionSet<Entity> and(final FieldCondition<?, Entity> fieldCondition) {
        return ConditionSet.and(this, fieldCondition);
    }

    /**
     * Combines conditions using a logical 'or'
     *
     * @param fieldCondition Condition for combine
     * @return A set of conditions for comparison
     */
    public ConditionSet<Entity> or(final FieldCondition<?, Entity> fieldCondition) {
        return ConditionSet.or(this, fieldCondition);
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
    public boolean fitsCondition(final Entity record) {
        try {
            return condition
                    .test((Fld) FieldUtil.getFieldFromName(record.getClass(), fieldName).get(record));
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }
}

