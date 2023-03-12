package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.DeadLockException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;
import io.github.alekseykn.imnorm.exceptions.TransactionWasClosedException;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Allows you to implement transactional behavior.
 * Allows you to avoid loss of data integrity during operations and be able to effectively roll back the changes made.
 * During an active transaction,
 * the data clusters accessed by the current transaction are blocked from all other transactions.
 * Implemented blocking and waiting transactions.
 * A blocking transaction waits for all other transactions to complete before starting its own.
 * Using only a blocking transaction ensures that no exception is thrown.
 * Waiting transactions will try to run in parallel. When accessing a cluster captured by another transaction,
 * the transaction will wait for it to be unlocked and, if the waiting time is too long, it will throw an exception.
 * If the thread from which the transaction was created is interrupted, the transaction is canceled automatically.
 *
 * @author Aleksey-Kn
 */
public class Transaction {
    /**
     * Set of open transaction need for auto-rollback transactions whose thread is no longer active
     */
    private final static Set<Transaction> openTransactions = ConcurrentHashMap.newKeySet();

    /**
     * An object to implement the waiting when creating blocking transaction
     */
    private final static Object mutex = new Object();

    static {
        Thread remover = new Thread(() -> {
            try {
                Thread.sleep(2000);
                Iterator<Transaction> iterator = openTransactions.iterator();
                Transaction now;
                while (iterator.hasNext()) {
                    now = iterator.next();
                    if (now.callingThreadIsDye()) {
                        now.rollback();
                        iterator.remove();
                    }
                }
            } catch (InterruptedException e) {
                throw new InternalImnormException(e);
            }
        });
        remover.setDaemon(true);
        remover.start();
    }

    /**
     * Open waiting transaction with specified time of wait.
     * When in such transaction accesses a blocked cluster,
     * it waits for the resource to be released for the specified time.
     * If this time is exceeded, a DeadLockException is thrown.
     *
     * @param waitBeforeThrowException Max wait time release cluster
     * @return New transaction
     */
    public static Transaction waitingTransaction(final int waitBeforeThrowException) {
        return new Transaction(waitBeforeThrowException);
    }

    /**
     * Open waiting transaction with 250 ms of wait.
     * When in such transaction accesses a blocked cluster,
     * it waits for the resource to be released for the 250 ms.
     * If this time is exceeded, a DeadLockException is thrown.
     *
     * @return New transaction
     */
    public static Transaction waitingTransaction() {
        return new Transaction(250);
    }

    /**
     * Waits for the completion of all other transactions, and then creates a new one.
     * Thus, it is guaranteed that when using only blocking transactions,
     * the possibility of throwing an DeadLockException is excluded.
     * However, the use of blocking transactions does not allow multiple transactions to work simultaneously,
     * which negatively affects performance.
     *
     * @return New transaction
     * @throws InternalImnormException The thread was abandoned while waiting for a new transaction to be created
     */
    public static Transaction blockingTransaction() {
        synchronized (mutex) {
            try {
                while (!openTransactions.isEmpty()) {
                    mutex.wait();
                }
            } catch (InterruptedException e) {
                throw new InternalImnormException(e);
            }
            return new Transaction(250);
        }
    }

    /**
     * Execute current procedure, automatically create, commit and flush or rollback waiting transaction.
     * If repository throw DeadLockException, rollback transaction and procedure retry.
     * If throw other exception, rollback transaction and return this exception.
     *
     * @param transactionalCall Procedure to be executed.
     *                          May be executed an unlimited number of times until an exception that is not a DeadLockException is thrown,
     *                          or the transaction completes successfully.
     * @return Exception, if procedure throw exception. Optional.empty() if procedure completed correctly.
     */
    public static Optional<Exception> executeInWaitingTransactionWithReply(final Consumer<Transaction> transactionalCall) {
        return executeInWaitingTransactionWithReply(transactionalCall, 250);
    }

    /**
     * Execute current procedure, automatically create, commit commit and flush or rollback waiting transaction.
     * If repository throw DeadLockException, rollback transaction and procedure retry.
     * If throw other exception, rollback transaction and return this exception.
     *
     * @param transactionalCall        Procedure to be executed.
     *                                 May be executed an unlimited number of times until an exception that is not a DeadLockException is thrown,
     *                                 or the transaction completes successfully.
     * @param waitBeforeThrowException Max time to wait for the resource to be released
     * @return Exception, if procedure throw exception. Optional.empty() if procedure completed correctly.
     */
    public static Optional<Exception> executeInWaitingTransactionWithReply(final Consumer<Transaction> transactionalCall,
                                                                           final int waitBeforeThrowException) {
        Transaction transaction;
        while (true) {
            transaction = new Transaction(waitBeforeThrowException);
            try {
                transactionalCall.accept(transaction);
                transaction.commitAndFlush();
                return Optional.empty();
            } catch (DeadLockException ignore) {
            } catch (Exception e) {
                transaction.rollback();
                return Optional.of(e);
            }
        }
    }


    /**
     * Thread, from which was created this transaction
     */
    private final Thread callingThread;

    /**
     * Clusters owned by this transaction
     */
    private Set<Cluster<?>> blockingClusters = new HashSet<>();

    /**
     * The waiting time for the cluster to be released, if exceeded, it will be thrown DeadLockException
     */
    @Getter(value = AccessLevel.PACKAGE)
    private final int waitTime;

    /**
     * @param waitBeforeThrow The waiting time for the cluster to be released,
     *                        if exceeded, it will be thrown DeadLockException
     */
    private Transaction(final int waitBeforeThrow) {
        callingThread = Thread.currentThread();
        openTransactions.add(this);
        waitTime = waitBeforeThrow;
    }

    /**
     * Marks the current cluster as blocked by this transaction.
     * Other transactions will not be able to access this cluster until the current transaction is completed.
     *
     * @param cluster Cluster, subject to blocking
     * @throws TransactionWasClosedException Accessing a transaction after it is closed
     */
    void captureLock(final Cluster<?> cluster) {
        if (Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        blockingClusters.add(cluster);
    }

    /**
     * Checks the current cluster as blocked by this transaction
     *
     * @param cluster The cluster being checked
     * @return True, if this transaction own current cluster
     * @throws TransactionWasClosedException Accessing a transaction after it is closed
     */
    boolean lockOwner(final Cluster<?> cluster) {
        if (Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        return blockingClusters.contains(cluster);
    }

    /**
     * Save all changes, made in this transaction
     *
     * @throws TransactionWasClosedException Accessing a transaction after it is closed
     */
    public void commit() {
        if (Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        blockingClusters.forEach(Cluster::commit);
        blockingClusters = null;
        openTransactions.remove(this);
        synchronized (mutex) {
            mutex.notify();
        }
    }

    /**
     * Save all changes, made in this transaction, and flush changes to file data storage
     *
     * @throws TransactionWasClosedException Accessing a transaction after it is closed
     */
    public void commitAndFlush() {
        if (Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        blockingClusters.forEach(Cluster::commit);
        blockingClusters.stream().map(Cluster::getRepository).distinct().forEach(Repository::flush);
        blockingClusters = null;
        openTransactions.remove(this);
        synchronized (mutex) {
            mutex.notify();
        }
    }

    /**
     * Cancel all changes, made in this transaction
     *
     * @throws TransactionWasClosedException Accessing a transaction after it is closed
     */
    public void rollback() {
        if (Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        blockingClusters.forEach(Cluster::rollback);
        blockingClusters = null;
        openTransactions.remove(this);
        synchronized (mutex) {
            mutex.notify();
        }
    }

    /**
     * Checking for the existence of the thread from which the current transaction was created
     *
     * @return True, if the thread from which the current transaction was created is dead
     */
    private boolean callingThreadIsDye() {
        return !callingThread.isAlive();
    }
}

