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
                    if(now.callingThreadIsDye()) {
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
    private final Map<Cluster<?>, Map<String, Object>> blockedRecord = new HashMap<>();

    public Transaction() {
        callingThread = Thread.currentThread();
        openTransactions.add(this);
    }

    protected void backup(Cluster<?> provenance, String recordId, Object nowValue) {
        if (!blockedRecord.containsKey(provenance)) {
            blockedRecord.put(provenance, new HashMap<>());
        }
        blockedRecord.get(provenance).put(recordId, nowValue);
    }

    protected boolean recordWasLockedCurrentTransaction(Cluster<?> provenance, String recordId) {
        if(blockedRecord.containsKey(provenance)) {
            return blockedRecord.get(provenance).containsKey(recordId);
        } else return false;
    }

    public void commit() {
        blockedRecord.forEach((repository, stringObjectMap) -> repository.unlock(stringObjectMap.keySet()));
    }

    public void rollback() {
        blockedRecord.forEach(Cluster::rollback);
    }

    protected boolean callingThreadIsDye() {
       return !callingThread.isAlive();
    }
}
