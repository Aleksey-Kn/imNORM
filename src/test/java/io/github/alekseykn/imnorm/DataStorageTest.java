package io.github.alekseykn.imnorm;

import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import support.dto.Dto;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class DataStorageTest {
    
    @BeforeAll
    @SneakyThrows
    static void removeTestStorage() {
        deleteAll(new File("test1"));
        deleteAll(new File("test2"));
        deleteAll(new File("test3"));
        deleteAll(new File("test4"));
    }

    private static void deleteAll(File file) {
        if (file.isDirectory()) {
            Arrays.stream(Objects.requireNonNull(file.listFiles())).forEach(DataStorageTest::deleteAll);
        }
        file.delete();
    }

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
                .getStrictlyFrugalRepositoryForClass(Dto.class, 1) instanceof FrugalRepository)
                .isTrue();
    }
    
    @Test
    void executeNewMigrations() {
        DataStorage dataStorage = DataStorage.getDataStorage(Path.of("test1"));
        dataStorage.executeMigration("1", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(3), transaction));
        dataStorage.executeMigration("2", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(5), transaction));
        dataStorage.executeMigration("3", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(7), transaction));

        assertThat(dataStorage.getRepositoryForClass(Dto.class).findAll()).extracting(Dto::getId).contains(3, 5, 7);
    }

    @Test
    void executeRecurringMigrations() {
        DataStorage dataStorage = DataStorage.getDataStorage(Path.of("test2"));
        dataStorage.executeMigration("1", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(3), transaction));
        dataStorage.executeMigration("2", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(5), transaction));
        dataStorage.executeMigration("1", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(7), transaction));
        dataStorage.executeMigration("2", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(11), transaction));
        dataStorage.executeMigration("1", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(66), transaction));

        assertThat(dataStorage.getRepositoryForClass(Dto.class).findAll()).extracting(Dto::getId).containsOnly(3, 5);
    }

    @Test
    void migrationsWithErrorsAreNotMarkedAsCompletedAndAreRollback() {
        DataStorage dataStorage = DataStorage.getDataStorage(Path.of("test3"));
        dataStorage.executeMigration("1", (ds, transaction) -> {
            throw new RuntimeException();
        });
        dataStorage.executeMigration("2", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(5), transaction));
        dataStorage.executeMigration("1", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(7), transaction));

        assertThat(dataStorage.getRepositoryForClass(Dto.class).findAll()).extracting(Dto::getId).contains(5, 7);
    }

    @Test
    void migrationsWithErrorsNotSaveChanges() {
        DataStorage dataStorage = DataStorage.getDataStorage(Path.of("test4"));
        dataStorage.executeMigration("1", (ds, transaction) -> {
            ds.getRepositoryForClass(Dto.class).save(new Dto(111), transaction);
            throw new RuntimeException();
        });
        dataStorage.executeMigration("2", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(5), transaction));
        dataStorage.executeMigration("1", (ds, transaction) -> ds.getRepositoryForClass(Dto.class)
                .save(new Dto(7), transaction));

        assertThat(dataStorage.getRepositoryForClass(Dto.class).findAll()).extracting(Dto::getId).contains(5, 7);
    }
}
