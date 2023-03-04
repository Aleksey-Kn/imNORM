package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.OtherTransactionRedactCurrentClusterException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.dto.Dto;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionTest {
    private final static Repository<Dto> repository = DataStorage.getDataStorage().getRepositoryForClass(Dto.class);

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }

    @Test
    void saveShouldWordWithOneTransaction() {
        Transaction transaction = Transaction.waitTransaction();
        Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .forEach(id -> repository.save(new Dto(id), transaction));
        transaction.commit();
        assertThat(repository.findAll().size()).isEqualTo(100);
    }

    @Test
    void saveShouldWordWithOneTransactionWithRollback() {
        repository.save(new Dto(10));
        repository.save(new Dto(40));
        Transaction transaction = Transaction.waitTransaction();
        Stream.iterate(0, integer -> integer + 1)
                .limit(100)
                .forEach(id -> repository.save(new Dto(id), transaction));
        transaction.rollback();
        assertThat(repository.findAll()).extracting(Dto::getId).containsOnly(10, 40);
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInOneClusterFromWaitingTransaction() {
        Set<Integer> first = Stream.iterate(10000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(20000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.waitTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        });
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.waitTransaction();
            second.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        assertThat(repository.findAll().size())
                .isEqualTo(200);
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInOneClusterFromWaitingTransactionWithOneRollback() {
        Set<Integer> first = Stream.iterate(10000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(20000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.waitTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.rollback();
        });
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.waitTransaction();
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
    void saveShouldThrowExceptionWithTwoThreadWritingInOneClusterFromNoWaitingTransaction() {
        Set<Integer> first = Stream.iterate(0, integer -> integer + 1)
                .limit(10)
                .collect(Collectors.toSet());
        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.noWaitTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            transaction.commit();
        });
        thread1.start();
        Thread.sleep(500);
        assertThatThrownBy(() -> {
            Transaction transaction = Transaction.noWaitTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }).isInstanceOf(OtherTransactionRedactCurrentClusterException.class);
    }

    @Test
    @SneakyThrows
    void saveShouldWorkWithTwoThreadWritingInDifferentClusterFromNoWaitingTransaction() {
        Set<Integer> first = Stream.iterate(10000, integer -> integer + 1)
                .limit(100)
                .collect(Collectors.toSet());
        Set<Integer> second = Stream.iterate(20001, integer -> integer + 1)
                .limit(99)
                .collect(Collectors.toSet());
        repository.save(new Dto(20000));
        Thread thread1 = new Thread(() -> {
            Transaction transaction = Transaction.noWaitTransaction();
            first.forEach(id -> repository.save(new Dto(id), transaction));
            transaction.commit();
        }, "First");
        Thread thread2 = new Thread(() -> {
            Transaction transaction = Transaction.noWaitTransaction();
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