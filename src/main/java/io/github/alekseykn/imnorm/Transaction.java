package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.MultipleAccessToCluster;
import io.github.alekseykn.imnorm.exceptions.TransactionWasClosedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class Transaction {
    private final static Set<Transaction> openTransactions = ConcurrentHashMap.newKeySet();

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
                e.printStackTrace();
            }
        });
        remover.setDaemon(true);
        remover.start();
    }

    public static Transaction noWaitTransaction() {
        return new Transaction();
    }

    public static Transaction waitTransaction() {
        synchronized (mutex) {
            try {
                while (!openTransactions.isEmpty()) {
                    mutex.wait(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return new Transaction();
        }
    }

    public static Optional<Exception> executeInNoWaitTransactionWithReply(Consumer<Transaction> transactionalCall) {
        Transaction transaction;
        while (true) {
            transaction = new Transaction();
            try {
                transactionalCall.accept(transaction);
                transaction.commit();
                return Optional.empty();
            } catch (MultipleAccessToCluster ignore) {
            } catch (Exception e) {
                transaction.rollback();
                return Optional.of(e);
            }
        }
    }


    private final Thread callingThread;
    private Set<Cluster<?>> blockingClusters = new HashSet<>();

    private Transaction() {
        callingThread = Thread.currentThread();
        openTransactions.add(this);
    }

    void captureLock(Cluster<?> provenance) {
        if(Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        blockingClusters.add(provenance);
    }

    boolean lockOwner(Cluster<?> provenance) {
        if(Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        return blockingClusters.contains(provenance);
    }

    public void commit() {
        if(Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        blockingClusters.forEach(Cluster::commit);
        blockingClusters = null;
        openTransactions.remove(this);
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public void rollback() {
        if(Objects.isNull(blockingClusters))
            throw new TransactionWasClosedException();
        blockingClusters.forEach(Cluster::rollback);
        blockingClusters = null;
        openTransactions.remove(this);
        synchronized (mutex) {
            mutex.notify();
        }
    }

    private boolean callingThreadIsDye() {
        return !callingThread.isAlive();
    }
}
