package io.github.alekseykn.imnorm.where;

import org.junit.jupiter.api.Test;
import support.dto.DtoWithGenerateId;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionSetTest {

    @Test
    void andShouldReturnTrue() {
        assertThat(ConditionSet.and(new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 0),
                        new FieldCondition<>("id", CompareMode.LESS, 15))
                .and(new FieldCondition<>("id", CompareMode.MORE, 5))
                .fitsCondition(new DtoWithGenerateId(10, 2))).isTrue();
    }

    @Test
    void andShouldReturnFalse() {
        assertThat(ConditionSet.and(new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 1),
                        new FieldCondition<>("id", CompareMode.LESS, 10))
                .and(new FieldCondition<>("id", CompareMode.MORE, 1))
                .fitsCondition(new DtoWithGenerateId(16, 3))).isFalse();
    }

    @Test
    void orShouldReturnTrue() {
        assertThat(ConditionSet.or(new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 1),
                        new FieldCondition<>("id", CompareMode.LESS, 10))
                .or(new FieldCondition<>("id", CompareMode.MORE, 1))
                .fitsCondition(new DtoWithGenerateId(16, 2))).isTrue();
    }

    @Test
    void orShouldReturnFalse() {
        assertThat(ConditionSet.or(new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 1),
                        new FieldCondition<>("id", CompareMode.EQUALS, 10))
                .or(new FieldCondition<>("id", CompareMode.NOT_EQUALS, 1))
                .fitsCondition(new DtoWithGenerateId(1, 2))).isFalse();
    }

    @Test
    void fitsConditionShouldReturnTrue() {
        assertThat(ConditionSet.or(new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 1),
                        new FieldCondition<>("id", CompareMode.LESS, 10))
                .and(new FieldCondition<>("id", CompareMode.MORE, 5))
                .fitsCondition(new DtoWithGenerateId(2, 3))).isTrue();
    }

    @Test
    void fitsConditionShouldReturnFalse() {
        assertThat(ConditionSet.or(new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 0),
                        new FieldCondition<>("id", CompareMode.EQUALS, 10))
                .and(new FieldCondition<>("id", CompareMode.MORE, 5))
                .fitsCondition(new DtoWithGenerateId(7, 3))).isFalse();
    }

    @Test
    void fitsConditionWithNestedConditionShouldReturnTrue() {
        assertThat(ConditionSet.and(
                        ConditionSet.or(
                                new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 1),
                                new FieldCondition<>("id", CompareMode.EQUALS, 7)),
                        ConditionSet.or(
                                new FieldCondition<Integer, DtoWithGenerateId>("number", n -> n < 10 && n > 5),
                                new FieldCondition<>("id", CompareMode.MORE, 100)))
                .fitsCondition(new DtoWithGenerateId(7, 8))).isTrue();
    }

    @Test
    void fitsConditionWithNestedConditionShouldReturnFalse() {
        assertThat(ConditionSet.and(
                        ConditionSet.or(
                                new FieldCondition<Integer, DtoWithGenerateId>("number", n -> (n & 1) == 0),
                                new FieldCondition<>("id", CompareMode.LESS, 0)),
                new FieldCondition<>("id", CompareMode.MORE, 10))
                .fitsCondition(new DtoWithGenerateId(7, 8))).isFalse();
    }
}
