package io.github.alekseykn.imnorm.where;

/**
 * Interface for checking the passage of the current object under the condition
 */
public interface Condition {
    /**
     * Checks whether the passed object meets the condition
     * @param record The object being checked
     * @return True, if the object fits the condition
     */
    boolean fitsCondition(Object record);
}
