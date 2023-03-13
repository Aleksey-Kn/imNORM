package io.github.alekseykn.imnorm.where;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class ConditionSet implements Condition {
    private final Where<?> worker;

    public ConditionSet and(Where<?> where) {
        return worker.and(where);
    }

    public ConditionSet or(Where<?> where) {
        return worker.or(where);
    }

    @Override
    public boolean fitsCondition(final Object record) {
        return worker.fitsCondition(record);
    }
}
