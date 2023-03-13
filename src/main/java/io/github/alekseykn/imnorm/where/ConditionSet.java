package io.github.alekseykn.imnorm.where;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public final class ConditionSet implements Condition {
    private final List<Boolean> operationIsAnd;
    private final List<Condition> conditions;

    private ConditionSet(final boolean isAndOperation, final List<Condition> operands) {
        operationIsAnd = new LinkedList<>(List.of(isAndOperation));
        conditions = new LinkedList<>(operands);
    }

    static ConditionSet and(final Where<?> first, final Where<?> second) {
        return new ConditionSet(true, List.of(first, second));
    }

    static ConditionSet or(final Where<?> first, final Where<?> second) {
        return new ConditionSet(false, List.of(first, second));
    }


    public ConditionSet and(final Condition condition) {
        operationIsAnd.add(true);
        conditions.add(condition);
        return this;
    }
    
    public ConditionSet or(final Condition condition) {
        operationIsAnd.add(false);
        conditions.add(condition);
        return this;
    }

    @Override
    public boolean fitsCondition(final Object record) {
        List<Boolean> nowOperands = conditions.stream()
                .map(condition -> condition.fitsCondition(record))
                .collect(Collectors.toList());
        List<Boolean> nowOperations = new ArrayList<>(operationIsAnd);
        for(int i = 0; i < nowOperations.size();) {
            if(nowOperations.get(i)) {
                nowOperands.set(i, nowOperands.get(i) && nowOperands.remove(i + 1));
                nowOperations.remove(i);
            } else {
                i++;
            }
        }
        return nowOperands.stream().anyMatch(b -> b);
    }
}
