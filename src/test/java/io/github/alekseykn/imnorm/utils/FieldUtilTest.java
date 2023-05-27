package io.github.alekseykn.imnorm.utils;

import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import support.dto.ChildDto;
import support.dto.Dto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class FieldUtilTest {

    @Test
    @SneakyThrows
    void getIdFieldFromCurrentClass() {
        assertThat(FieldUtil.getIdField(Dto.class)).isEqualTo(Dto.class.getDeclaredField("id"));
    }

    @Test
    @SneakyThrows
    void getIdFieldFromParentClass() {
        assertThat(FieldUtil.getIdField(ChildDto.class)).isEqualTo(Dto.class.getDeclaredField("id"));
    }

    @Test
    @SneakyThrows
    void getFieldFromNameParentClass() {
        assertThat(FieldUtil.getFieldFromName(ChildDto.class, "id"))
                .isEqualTo(Dto.class.getDeclaredField("id"));
    }

    @Test
    @SneakyThrows
    void getFieldFromNameCurrentClass() {
        assertThat(FieldUtil.getFieldFromName(ChildDto.class, "odd"))
                .isEqualTo(ChildDto.class.getDeclaredField("odd"));
    }

    @Test
    void countFields() {
        assertThat(FieldUtil.countFields(ChildDto.class)).isEqualTo(2);
    }
}