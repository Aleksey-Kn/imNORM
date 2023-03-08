package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import support.dto.Dto;

import static org.junit.jupiter.api.Assertions.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FastRepositoryTest {
    private final static Repository<Dto> repository = DataStorage.getDataStorage().getRepositoryForClass(Dto.class);

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }

    @BeforeEach
    void before() {
        repository.save(new Dto(5));
        repository.save(new Dto(-1));
        repository.save(new Dto(25));
    }

    @Test
    void save() {
        repository.save(new Dto(18));

        assertThat(repository.findAll()).extracting(Dto::getId).contains(-1, 5, 18, 25);
    }

    @Test
    void findById() {
        assertThat(repository.findById(5)).isEqualTo(new Dto(5));
    }

    @Test
    void deleteById() {
        repository.deleteById(-1);

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(5, 25);
    }

    @Test
    void delete() {
        repository.delete(new Dto(5));

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(-1, 25);
    }

    @Test
    void findAll() {
    }

    @Test
    void deleteAll() {
    }

    @Test
    void flush() {
    }
}