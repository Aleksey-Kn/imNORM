package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import support.dto.Dto;
import support.dto.DtoWithGenerateId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

abstract class RepositoryTest {
    protected static Repository<Dto> repository;
    protected static Repository<DtoWithGenerateId> withGenerateIdRepository;

    @AfterEach
    void tearDown() {
        repository.deleteAll();
        withGenerateIdRepository.deleteAll();
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
    void saveWithGenerateId() {
        withGenerateIdRepository.save(new DtoWithGenerateId(1));
        withGenerateIdRepository.save(new DtoWithGenerateId(2));
        withGenerateIdRepository.save(new DtoWithGenerateId(4));

        assertThat(withGenerateIdRepository.findAll()).extracting(DtoWithGenerateId::getNumber).contains(1, 2, 4);
    }

    @Test
    void saveWithPartialGenerateId() {
        withGenerateIdRepository.save(new DtoWithGenerateId(1));
        withGenerateIdRepository.save(new DtoWithGenerateId(10, 2));
        withGenerateIdRepository.save(new DtoWithGenerateId(4));

        assertThat(withGenerateIdRepository.findAll()).extracting(DtoWithGenerateId::getNumber).contains(1, 2, 4);
        assertThat(withGenerateIdRepository.findAll()).extracting(DtoWithGenerateId::getId).contains(10);
    }

    @Test
    void saveWithGenerateIdWithCommitTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        withGenerateIdRepository.save(new DtoWithGenerateId(1), transaction);
        withGenerateIdRepository.save(new DtoWithGenerateId(2), transaction);
        withGenerateIdRepository.save(new DtoWithGenerateId(4), transaction);
        transaction.commit();

        assertThat(withGenerateIdRepository.findAll()).extracting(DtoWithGenerateId::getNumber).contains(1, 2, 4);
    }

    @Test
    void saveWithGenerateIdWithRollbackTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        withGenerateIdRepository.save(new DtoWithGenerateId(1), transaction);
        withGenerateIdRepository.save(new DtoWithGenerateId(2), transaction);
        withGenerateIdRepository.save(new DtoWithGenerateId(4), transaction);
        transaction.rollback();

        assertThat(withGenerateIdRepository.findAll()).isEmpty();
    }

    @Test
    void saveAll() {
        List<Dto> list = List.of(new Dto(2), new Dto(4), new Dto(8));

        repository.saveAll(list);

        assertThat(repository.findAll()).extracting(Dto::getId).contains(-1, 5, 2, 4, 8, 25);
    }

    @Test
    void saveAllWithCommitTransaction() {
        List<Dto> list = List.of(new Dto(2), new Dto(4), new Dto(8));

        Transaction transaction = Transaction.waitingTransaction();
        repository.saveAll(list, transaction);
        transaction.commit();

        assertThat(repository.findAll()).extracting(Dto::getId).contains(-1, 5, 2, 4, 8, 25);
    }

    @Test
    void saveAllWithRollbackTransaction() {
        List<Dto> list = List.of(new Dto(2), new Dto(4), new Dto(8));

        Transaction transaction = Transaction.waitingTransaction();
        repository.saveAll(list, transaction);
        transaction.rollback();

        assertThat(repository.findAll()).extracting(Dto::getId).contains(-1, 5, 25);
    }

    @Test
    void saveAllWithFullGenerateId() {
        List<DtoWithGenerateId> list = List.of(new DtoWithGenerateId(2),
                new DtoWithGenerateId(4),
                new DtoWithGenerateId(8));

        withGenerateIdRepository.saveAll(list);

        assertThat(withGenerateIdRepository.findAll()).extracting(DtoWithGenerateId::getNumber).contains(2, 4, 8);
    }

    @Test
    void saveAllWithPartialGenerateId() {
        List<DtoWithGenerateId> list = List.of(new DtoWithGenerateId(2),
                new DtoWithGenerateId(33, 4),
                new DtoWithGenerateId(8));

        withGenerateIdRepository.saveAll(list);

        assertThat(withGenerateIdRepository.findAll()).extracting(DtoWithGenerateId::getNumber).contains(2, 4, 8);
        assertThat(withGenerateIdRepository.findAll()).extracting(DtoWithGenerateId::getId).contains(33);
    }

    @Test
    void saveWithCommitTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        repository.save(new Dto(18), transaction);
        repository.save(new Dto(36), transaction);
        transaction.commit();

        assertThat(repository.findAll()).extracting(Dto::getId).contains(-1, 5, 18, 25, 36);
    }

    @Test
    void saveWithRollbackTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        repository.save(new Dto(16), transaction);
        repository.save(new Dto(38), transaction);
        transaction.rollback();

        assertThat(repository.findAll()).extracting(Dto::getId).contains(-1, 5, 25);
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
    void deleteByIdWithCommitTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        repository.deleteById(-1, transaction);
        transaction.commit();

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(5, 25);
    }

    @Test
    void deleteByIdWithRollbackTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        repository.deleteById(-1, transaction);
        repository.deleteById(25, transaction);
        transaction.rollback();

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(-1, 5, 25);
    }

    @Test
    void delete() {
        repository.delete(new Dto(5));

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(-1, 25);
    }

    @Test
    void deleteWithCommitTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        repository.delete(new Dto(5), transaction);
        transaction.commit();

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(-1, 25);
    }

    @Test
    void deleteWithRollbackTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        repository.delete(new Dto(5), transaction);
        repository.delete(new Dto(-1), transaction);
        transaction.rollback();

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(5, -1, 25);
    }

    @Test
    void findAll() {
        assertThat(repository.findAll()).extracting(Dto::getId).contains(5, -1, 25);
    }

    @Test
    void findAllWithPaginationSmallRowCount() {
        assertThat(repository.findAll(1, 1)).extracting(Dto::getId).containsOnly(25);
    }

    @Test
    void findAllWithPaginationBigRowCount() {
        assertThat(repository.findAll(1, 3)).extracting(Dto::getId).containsOnly(5, 25);
    }

    @Test
    void deleteAll() {
        repository.deleteAll();

        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    void flush() {
        repository.flush();

        assertThat(Arrays.stream(Objects.requireNonNull(Path
                        .of("data", Dto.class.getName().replace('.', '_')).toFile()
                        .listFiles((dir, name) -> !name.equals("_sequence.imnorm"))))
                .flatMap(file -> {
                    try {
                        return Files.lines(file.toPath());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).count()).isEqualTo(3);
    }
    
    @Test
    void findAllWithCondition() {
        assertThat(repository.findAll(new FieldCondition<>("id", CompareMode.MORE, 0)))
                .extracting(Dto::getId).containsOnly(5, 25);
    }

    @Test
    void findAllWithConditionAndPagination() {
        repository.save(new Dto(100));

        assertThat(repository.findAll(new FieldCondition<>("id", CompareMode.MORE, 0), 1, 1))
                .extracting(Dto::getId).containsOnly(25);
    }
    
    @Test
    void findAllWithLambdaCondition() {
        assertThat(repository.findAll(record -> record.equals(new Dto(-1)))).extracting(Dto::getId).containsOnly(-1);
    }
}
