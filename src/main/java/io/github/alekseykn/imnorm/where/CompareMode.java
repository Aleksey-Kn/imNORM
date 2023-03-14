package io.github.alekseykn.imnorm.where;

import java.util.Comparator;
import java.util.function.Predicate;

/**
 * Implementation of the basic conditions for comparing objects
 *
 * @author Aleksey-Kn
 */
public enum CompareMode {
    // Invert 'more' and 'less' because of the different  order of the arguments in Where class
    EQUALS(cr -> cr == 0), NOT_EQUALS(cr -> cr != 0), MORE(cr -> cr < 0), LESS(cr -> cr > 0);

    /**
     * Current conditions for comparing
     */
    private final Predicate<Integer> condition;

    CompareMode(final Predicate<Integer> cond) {
        condition = cond;
    }

    /**
     * Checks whether the relation of these objects fits the specified condition
     *
     * @param first  The object being compared
     * @param second The object being compared
     * @param <T>    Comparing type
     * @return True, if objects fits the condition
     */
    <T> boolean checkCondition(Comparable<T> first, T second) {
        return condition.test(first.compareTo(second));
    }

    /**
     * Checks whether the relation of these objects fits the specified condition based on the comparator rules
     *
     * @param first      The object being compared
     * @param second     The object being compared
     * @param <T>        Comparing type
     * @param comparator Rule for comparison
     * @return True, if objects fits the condition
     */
    <T> boolean checkCondition(T first, T second, Comparator<T> comparator) {
        return condition.test(comparator.compare(first, second));
    }
}
