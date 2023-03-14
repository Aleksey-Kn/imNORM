package io.github.alekseykn.imnorm.where;

import org.junit.jupiter.api.Test;
import support.dto.Dto;

import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

class FieldConditionTest {

    @Test
    void andReturnTrue() {
        assertThat(new FieldCondition<Integer, Dto>("id", id -> id > 5)
                .and(new FieldCondition<Integer, Dto>("id", id -> id < 10)).fitsCondition(new Dto(7)))
                .isTrue();
    }

    @Test
    void andReturnFalse() {
        assertThat(new FieldCondition<Integer, Dto>("id", id -> id > 5)
                .and(new FieldCondition<Integer, Dto>("id", id -> id < 10)).fitsCondition(new Dto(12)))
                .isFalse();
    }

    @Test
    void orReturnTrue() {
        assertThat(new FieldCondition<Integer, Dto>("id", id -> id > 5)
                .or(new FieldCondition<Integer, Dto>("id", id -> id < 10)).fitsCondition(new Dto(12)))
                .isTrue();
    }

    @Test
    void orReturnFalse() {
        assertThat(new FieldCondition<Integer, Dto>("id", id -> (id & 1) == 1)
                .or(new FieldCondition<Integer, Dto>("id", id -> id < 10)).fitsCondition(new Dto(12)))
                .isFalse();
    }

    @Test
    void fitsConditionWithCompareModeMustReturnTrue() {
        assertThat(new FieldCondition<>("id", CompareMode.EQUALS, 10).fitsCondition(new Dto(10)))
                .isTrue();
    }

    @Test
    void fitsConditionWithCompareModeMustReturnFalse() {
        assertThat(new FieldCondition<>("id", CompareMode.MORE, 10).fitsCondition(new Dto(-1)))
                .isFalse();
    }

    @Test
    void fitsConditionWithComparatorMustReturnTrue() {
        assertThat(new FieldCondition<>("id", CompareMode.NOT_EQUALS, 10,
                Comparator.comparing(integer -> Integer.toString(integer).length())).fitsCondition(new Dto(100)))
                .isTrue();
    }

    @Test
    void fitsConditionWithComparatorModeMustReturnFalse() {
        assertThat(new FieldCondition<>("id", CompareMode.LESS, 99,
                Comparator.comparing(integer -> Integer.toString(integer).length())).fitsCondition(new Dto(-50)))
                .isFalse();
    }

    @Test
    void fitsConditionWithPredicateModeMustReturnTrue() {
        assertThat(new FieldCondition<Integer, Dto>("id", id -> (id & 1) == 0).fitsCondition(new Dto(10)))
                .isTrue();
    }

    @Test
    void fitsConditionWithPredicateModeMustReturnFalse() {
        assertThat(new FieldCondition<Integer, Dto>("id", id -> id % 2 == 1).fitsCondition(new Dto(10)))
                .isFalse();
    }
}