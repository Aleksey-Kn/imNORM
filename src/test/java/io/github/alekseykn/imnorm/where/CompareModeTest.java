package io.github.alekseykn.imnorm.where;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

class CompareModeTest {

    @Test
    void checkConditionFromEqualsModeShouldReturnTrue() {
        assertThat(CompareMode.EQUALS.checkCondition("aaa", "aaa")).isTrue();
    }

    @Test
    void checkConditionFromEqualsModeShouldReturnFalse() {
        assertThat(CompareMode.EQUALS.checkCondition(11, 22)).isFalse();
    }

    @Test
    void testCheckConditionFromEqualsMode() {
        assertThat(CompareMode.EQUALS.checkCondition("aaa", "bcd", Comparator.comparing(String::length)))
                .isTrue();
    }

    @Test
    void checkConditionFromNotEqualsModeShouldReturnTrue() {
        assertThat(CompareMode.NOT_EQUALS.checkCondition(11, 22)).isTrue();
    }

    @Test
    void checkConditionFromNotEqualsModeShouldReturnFalse() {
        assertThat(CompareMode.NOT_EQUALS.checkCondition("aaa", "aaa")).isFalse();
    }

    @Test
    void testCheckConditionFromNotEqualsMode() {
        assertThat(CompareMode.NOT_EQUALS.checkCondition("aaa", "bcd", Comparator.comparing(String::length)))
                .isFalse();
    }

    @Test
    void checkConditionFromMoreModeShouldReturnTrue() {
        assertThat(CompareMode.MORE.checkCondition("aaa", "aab")).isTrue();
    }

    @Test
    void checkConditionFromMoreModeShouldReturnFalse() {
        assertThat(CompareMode.MORE.checkCondition(3, 1)).isFalse();
    }

    @Test
    void testCheckConditionFromMoreMode() {
        assertThat(CompareMode.MORE.checkCondition("aaaaa", "bcd", Comparator.comparing(String::length)))
                .isFalse();
    }

    @Test
    void checkConditionFromLessModeShouldReturnTrue() {
        assertThat(CompareMode.LESS.checkCondition(LocalDate.now(), LocalDate.now().minusDays(1))).isTrue();
    }

    @Test
    void checkConditionFromLessModeShouldReturnFalse() {
        assertThat(CompareMode.LESS.checkCondition(LocalTime.now(), LocalTime.now().plusHours(2))).isFalse();
    }

    @Test
    void testCheckConditionFromLessMode() {
        assertThat(CompareMode.LESS.checkCondition(LocalDate.now(), LocalDate.now().minusDays(1),
                Comparator.comparing(LocalDate::getYear))).isFalse();
    }
}
