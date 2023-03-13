package io.github.alekseykn.imnorm.where;

public interface Condition {
    boolean fitsCondition(Object record);
}
