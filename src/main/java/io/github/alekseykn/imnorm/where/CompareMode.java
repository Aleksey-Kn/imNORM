package io.github.alekseykn.imnorm.where;

import java.util.Comparator;
import java.util.function.Predicate;

public enum CompareMode {
    // Invert 'more' and 'less' because of the different  order of the arguments in Where class
    EQUALS(cr -> cr == 0), NOT_EQUALS(cr -> cr != 0), MORE(cr -> cr < 0), LESS(cr -> cr > 0);

    private final Predicate<Integer> condition;

    CompareMode(final Predicate<Integer> cond) {
        condition = cond;
    }

    <T> boolean checkCondition(Comparable<T> first, T second) {
        return condition.test(first.compareTo(second));
    }

    <T> boolean checkCondition(T first, T second, Comparator<T> comparator) {
        return condition.test(comparator.compare(first, second));
    }
}
