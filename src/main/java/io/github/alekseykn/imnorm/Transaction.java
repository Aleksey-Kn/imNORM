package io.github.alekseykn.imnorm;

import java.util.HashMap;
import java.util.Map;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Transaction {
    private final static Set<Transaction> openTransactions = ConcurrentHashMap.newKeySet();

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

    private final Thread callingThread;
    private final Map<Cluster<?>, Set<String>> blockingId = new HashMap<>();

    public Transaction() {
        callingThread = Thread.currentThread();
        openTransactions.add(this);
    }

    void captureLock(Cluster<?> provenance, String recordId) {
        if (!blockingId.containsKey(provenance)) {
            blockingId.put(provenance, new HashSet<>());
        }
        blockingId.get(provenance).add(recordId);
    }

    boolean lockOwner(Cluster<?> provenance, String recordId) {
        if (blockingId.containsKey(provenance)) {
            return blockingId.get(provenance).contains(recordId);
        } else return false;
    }

    public void commit() {
        blockingId.forEach((cluster, ids) -> cluster.commit(this, ids));
        blockingId.clear();
    }

    public void rollback() {
        blockingId.forEach(((cluster, ids) -> cluster.rollback(this, ids)));
        blockingId.clear();
    }

    protected boolean callingThreadIsDye() {
        return !callingThread.isAlive();
    }
}
