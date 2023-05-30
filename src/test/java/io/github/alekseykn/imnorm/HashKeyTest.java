package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.dto.StringDto;

import java.math.BigInteger;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class HashKeyTest {
    protected static Repository<StringDto> repository;

    @AfterEach
    void deleteAll() {
        repository.deleteAll();
    }

    @Test
    void saveRecordWithForbiddenSymbols() {
        String key = "<Key:\\key>";
        repository.save(new StringDto(key));
        repository.flush();

        assertThat(repository.findById(key).orElseThrow().getId()).isEqualTo(key);
    }

    @Test
    void saveAllRecordWithForbiddenSymbols() {
        String key = "/Key|";
        repository.saveAll(List.of(new StringDto(key)));
        repository.flush();

        assertThat(repository.findById(key).orElseThrow().getId()).isEqualTo(key);
    }

    @Test
    void deleteRecordWithForbiddenSymbols() {
        String key = "**aa/";
        repository.save(new StringDto(key));
        repository.flush();

        assertThat(repository.deleteById(key).orElseThrow().getId()).isEqualTo(key);
        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    void saveRecordWithLargeKey() {
        String key = BigInteger.valueOf(2).pow(500).toString();
        repository.save(new StringDto(key));
        repository.flush();

        assertThat(repository.findById(key).orElseThrow().getId()).isEqualTo(key);
    }

    @Test
    void saveAllRecordWithLargeKey() {
        String key = BigInteger.valueOf(2).pow(600).toString();
        repository.saveAll(List.of(new StringDto(key)));
        repository.flush();

        assertThat(repository.findById(key).orElseThrow().getId()).isEqualTo(key);
    }

    @Test
    void deleteRecordWitLargeKey() {
        String key = BigInteger.valueOf(2).pow(700).toString();
        repository.save(new StringDto(key));
        repository.flush();

        assertThat(repository.deleteById(key).orElseThrow().getId()).isEqualTo(key);
        assertThat(repository.findAll()).isEmpty();
    }
}
