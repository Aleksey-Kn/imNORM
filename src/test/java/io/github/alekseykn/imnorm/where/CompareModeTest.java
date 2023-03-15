package io.github.alekseykn.imnorm.where;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

class CompareModeTest {

    @Test
    void checkConditionFromEqualsModeReturnTrue() {
        assertThat(CompareMode.EQUALS.checkCondition("aaa", "aaa")).isTrue();
    }

    @Test
    void checkConditionFromEqualsModeReturnFalse() {
        assertThat(CompareMode.EQUALS.checkCondition(11, 22)).isFalse();
    }

    @Test
    void testCheckConditionFromEqualsMode() {
        assertThat(CompareMode.EQUALS.checkCondition("aaa", "bcd", Comparator.comparing(String::length)))
                .isTrue();
    }

    @Test
    void checkConditionFromNotEqualsModeReturnTrue() {
        assertThat(CompareMode.NOT_EQUALS.checkCondition(11, 22)).isTrue();
    }

    @Test
    void checkConditionFromNotEqualsModeReturnFalse() {
        assertThat(CompareMode.NOT_EQUALS.checkCondition("aaa", "aaa")).isFalse();
    }

    @Test
    void testCheckConditionFromNotEqualsMode() {
        assertThat(CompareMode.NOT_EQUALS.checkCondition("aaa", "bcd", Comparator.comparing(String::length)))
                .isFalse();
    }

    @Test
    void checkConditionFromMoreModeReturnTrue() {
        assertThat(CompareMode.MORE.checkCondition("aaa", "aab")).isTrue();
    }

    @Test
    void checkConditionFromMoreModeReturnFalse() {
        assertThat(CompareMode.MORE.checkCondition(3, 1)).isFalse();
    }

    @Test
    void testCheckConditionFromMoreMode() {
        assertThat(CompareMode.MORE.checkCondition("aaaaa", "bcd", Comparator.comparing(String::length)))
                .isFalse();
    }

    @Test
    void checkConditionFromLessModeReturnTrue() {
        assertThat(CompareMode.LESS.checkCondition(LocalDate.now(), LocalDate.now().minusDays(1))).isTrue();
    }

    @Test
    void checkConditionFromLessModeReturnFalse() {
        assertThat(CompareMode.LESS.checkCondition(LocalTime.now(), LocalTime.now().plusHours(2))).isFalse();
    }

    @Test
    void testCheckConditionFromLessMode() {
        assertThat(CompareMode.LESS.checkCondition(LocalDate.now(), LocalDate.now().minusDays(1),
                Comparator.comparing(LocalDate::getYear))).isFalse();
    }
}
