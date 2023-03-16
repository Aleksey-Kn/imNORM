package io.github.alekseykn.imnorm;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.exceptions.DeadLockException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.dto.Dto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionTest {
    private final Repository<Dto> repository = DataStorage.getDataStorage().getRepositoryForClass(Dto.class);
    private final Gson gson = new Gson();

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }

    @Test
    void saveShouldWorkWithOneTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .forEach(id -> repository.save(new Dto(id), transaction));
        transaction.commit();

        assertThat(repository.findAll().size()).isEqualTo(100);
    }

    @Test
    void saveShouldWorkWithOneTransactionWithRollback() {
        repository.save(new Dto(10));
        repository.save(new Dto(40));

        Transaction transaction = Transaction.waitingTransaction();
        Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .forEach(id -> repository.save(new Dto(id), transaction));
        transaction.rollback();

        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(10, 40);
    }

    @Test
    void saveShouldWorkWithCommitAndFlushTransaction() {
        Transaction transaction = Transaction.waitingTransaction();
        Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .forEach(id -> repository.save(new Dto(id), transaction));
        transaction.commitAndFlush();

        assertThat(repository.findAll().size()).isEqualTo(100);
        assertThat(Arrays.stream(Objects.requireNonNull(Path
                        .of("data", Dto.class.getName().replace('.', '_')).toFile()
                        .listFiles((dir, name) -> !name.equals("_sequence.imnorm"))))
                .flatMap(file -> {
                    try {
                        return Files.lines(file.toPath());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).count()).isEqualTo(100);
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInOneClusterFromBlockingTransaction() {
        Set<Integer> first = Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(60, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.blockingTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        });
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.blockingTransaction();
            second.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll().size())
                .isEqualTo(160);
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInOneClusterFromRetryingWaitTransactions() {
        Set<Integer> first = Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(40, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> Transaction.executeInWaitingTransactionWithRetry(transaction ->
                first.forEach(id -> repository.save(new Dto(id), transaction))));
        Thread thread2 = new Thread(() -> Transaction.executeInWaitingTransactionWithRetry(transaction ->
                second.forEach(id -> repository.save(new Dto(id), transaction))));
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll())
                .extracting(Dto::getId)
                .containsAll(Stream.iterate(0, integer -> integer + 1).limit(140).collect(Collectors.toSet()));
        assertThat(Arrays.stream(Objects.requireNonNull(Path
                        .of("data", Dto.class.getName().replace('.', '_')).toFile()
                        .listFiles((dir, name) -> !name.equals("_sequence.imnorm"))))
                .flatMap(file -> {
                    try {
                        return Files.lines(file.toPath()).map(s -> s.substring(s.indexOf(':') + 1));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })).containsAll(Stream.iterate(0, integer -> integer + 1)
                .limit(140)
                .map(integer -> gson.toJson(new Dto(integer)))
                .collect(Collectors.toSet()));
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInOneClusterFromWaitTransactionsWithoutDeadLock() {
        Set<Integer> first = Stream.iterate(0, integer -> integer + 1)
                .limit(50)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(30, integer -> integer + 1)
                .limit(50)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        });
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction();
            second.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll())
                .extracting(Dto::getId)
                .containsAll(Stream.iterate(0, integer -> integer + 1).limit(80).collect(Collectors.toSet()));
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInOneClusterFromWaitTransactionsWithoutDeadLockWithLongTimeWait() {
        Set<Integer> first = Stream.iterate(1000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(1060, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction(5000);
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }, "First");
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction(5000);
            second.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }, "Second");
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll())
                .extracting(Dto::getId)
                .containsAll(Stream.iterate(1000, integer -> integer + 1).limit(160).collect(Collectors.toSet()));
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInManyClustersFromWaitTransactionsWithoutDeadLockWithLongTimeWait() {
        Set<Integer> first = Stream.iterate(10000, integer -> integer + 1)
                .limit(1000)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(10600, integer -> integer + 1)
                .limit(1000)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction(6000);
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }, "First");
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction(6000);
            second.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }, "Second");
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll())
                .extracting(Dto::getId)
                .containsAll(Stream.iterate(10000, integer -> integer + 1).limit(1600).collect(Collectors.toSet()));
    }

    @Test
    @SneakyThrows
    void saveShouldRollbackWhereThrowExceptionFromRetryingWaitTransactions() {
        Set<Integer> first = Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(40, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> Transaction.executeInWaitingTransactionWithRetry(transaction -> {
            first.forEach(id -> repository.save(new Dto(id), transaction));
            throw new RuntimeException("Expected exception");
        }));
        Thread thread2 = new Thread(() -> Transaction.executeInWaitingTransactionWithRetry(transaction ->
                second.forEach(id -> repository.save(new Dto(id), transaction))));
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll())
                .extracting(Dto::getId)
                .containsAll(Stream.iterate(40, integer -> integer + 1).limit(100).collect(Collectors.toSet()));
        assertThat(Arrays.stream(Objects.requireNonNull(Path
                        .of("data", Dto.class.getName().replace('.', '_')).toFile()
                        .listFiles((dir, name) -> !name.equals("_sequence.imnorm"))))
                .flatMap(file -> {
                    try {
                        return Files.lines(file.toPath()).map(s -> s.substring(s.indexOf(':') + 1));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })).containsAll(Stream.iterate(40, integer -> integer + 1)
                        .limit(100)
                        .map(integer -> gson.toJson(new Dto(integer)))
                        .collect(Collectors.toSet()));
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInOneClusterFromBlockingTransactionWithOneRollback() {
        Set<Integer> first = Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(1000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.blockingTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.rollback();
        });
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.blockingTransaction();
            second.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll().size())
                .isEqualTo(100);
    }

    @Test
    @SneakyThrows
    void saveShouldThrowExceptionWithTwoThreadWritingInOneClusterFromWaitingTransaction() {
        Set<Integer> first = Stream.iterate(0, integer -> integer + 1)
                .limit(10)
                .collect(Collectors.toSet());

        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            transaction.commit();
        });
        thread1.start();
        Thread.sleep(500);

        assertThatThrownBy(() -> {
            Transaction transaction = Transaction.waitingTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }).isInstanceOf(DeadLockException.class);
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInDifferentClusterFromWaitingTransaction() {
        Set<Integer> first = Stream.iterate(10000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(20001, integer -> integer + 1)
                .limit(99)
                .collect(Collectors.toSet());
        repository.save(new Dto(20000));

        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }, "First");
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.waitingTransaction();
            second.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }, "Second");
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(repository.findAll().size()).isEqualTo(200);
    }
}