package io.github.alekseykn.imnorm.where;

/**
 * Interface for checking the passage of the current object under the condition
 *
 * @param <E> Entity type
 * @author Aleksey-Kn
 */
@FunctionalInterface
public interface Condition<E> {
    /**
     * Checks whether the passed record meets the condition
     *
     * @param record The record being checked
     * @return True, if the record fits the condition
     */
    boolean fitsCondition(E record);
}
