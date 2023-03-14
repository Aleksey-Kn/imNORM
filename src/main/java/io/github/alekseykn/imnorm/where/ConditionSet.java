package io.github.alekseykn.imnorm.where;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A set of conditions for checking one or more fields of the same entity
 *
 * @param <E> Entity type
 * @author Aleksey-Kn
 */
public final class ConditionSet<E> implements Condition<E> {
    /**
     * Multiple operations between conditions (true = 'and', false = 'or')
     */
    private final List<Boolean> operationIsAnd;

    /**
     * All components of this ConditionSet
     */
    private final List<Condition<E>> conditions;

    /**
     * Create set of condition from input atomic conditions, united specified operator
     *
     * @param isAndOperation  Operation between conditions
     * @param firstCondition  Condition to be added to the current set
     * @param secondCondition Condition to be added to the current set
     */
    private ConditionSet(final boolean isAndOperation, final Condition<E> firstCondition,
                         final Condition<E> secondCondition) {
        operationIsAnd = new LinkedList<>(List.of(isAndOperation));
        conditions = new LinkedList<>(List.of(firstCondition, secondCondition));
    }

    /**
     * Create set of condition from input atomic conditions, united logic 'and'
     *
     * @param first Condition for create set of condition
     * @param second Condition for create set of condition
     * @param <E> Entity type
     * @return Set of condition, united logic 'and'
     */
    public static <E> ConditionSet<E> and(final Condition<E> first, final Condition<E> second) {
        return new ConditionSet<>(true, first, second);
    }

    /**
     * Create set of condition from input atomic conditions, united logic 'or'
     *
     * @param first Condition for create set of condition
     * @param second Condition for create set of condition
     * @param <E> Entity type
     * @return Set of condition, united logic 'or'
     */
    public static <E> ConditionSet<E> or(final Condition<E> first, final Condition<E> second) {
        return new ConditionSet<>(false, first, second);
    }


    /**
     * Add specified condition through logic 'and' operator
     *
     * @param condition Condition to add
     * @return Current ConditionSet
     */
    public ConditionSet<E> and(final Condition<E> condition) {
        operationIsAnd.add(true);
        conditions.add(condition);
        return this;
    }

    /**
     * Add specified condition through logic 'or' operator
     *
     * @param condition Condition to add
     * @return Current ConditionSet
     */
    public ConditionSet<E> or(final Condition<E> condition) {
        operationIsAnd.add(false);
        conditions.add(condition);
        return this;
    }

    /**
     * Checks whether the passed record meets the conditions, contain in this condition set
     *
     * @param record The record being checked
     * @return True, if the record fits the condition
     */
    @Override
    public boolean fitsCondition(final E record) {
        List<Boolean> nowOperands = conditions.stream()
                .map(condition -> condition.fitsCondition(record))
                .collect(Collectors.toList());
        List<Boolean> nowOperations = new ArrayList<>(operationIsAnd);
        for (int i = 0; i < nowOperations.size(); ) {
            if (nowOperations.get(i)) {
                nowOperands.set(i, nowOperands.get(i) && nowOperands.remove(i + 1));
                nowOperations.remove(i);
            } else {
                i++;
            }
        }
        return nowOperands.stream().anyMatch(b -> b);
    }
}

