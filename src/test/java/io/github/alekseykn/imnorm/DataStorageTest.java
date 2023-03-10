package io.github.alekseykn.imnorm;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import support.dto.Dto;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class DataStorageTest {

    @Test
    void getDataStorage() {
        DataStorage.getDataStorage();

        File dataStorage = new File("data");
        assertThat(dataStorage.exists()).isTrue();
        assertThat(dataStorage.isDirectory()).isTrue();
    }

    @Test
    void testGetDataStorage() {
        DataStorage.getDataStorage(Path.of("tests/inner"));

        File dataStorage = new File("tests/inner");
        assertThat(dataStorage.exists()).isTrue();
        assertThat(dataStorage.isDirectory()).isTrue();
    }

    @Test
    void getStrictlyFastRepositoryForClass() {
        DataStorage.getDataStorage().getPreferablyFrugalRepositoryForClass(Dto.class, 1);

        assertThat(DataStorage.getDataStorage().getStrictlyFastRepositoryForClass(Dto.class) instanceof FastRepository)
                .isTrue();
    }

    @Test
    void getStrictlyFrugalRepositoryForClass() {
        DataStorage.getDataStorage().getPreferablyFastRepositoryForClass(Dto.class);

        assertThat(DataStorage.getDataStorage()
                .getPreferablyFrugalRepositoryForClass(Dto.class, 1) instanceof FastRepository)
                .isTrue();
    }
}