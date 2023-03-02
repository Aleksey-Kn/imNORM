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
    private final Map<Cluster<?>, Map<String, Object>> redactedInTransactionRecord = new HashMap<>();
    private final Map<Cluster<?>, Set<String>> addedInTransactionRecord = new HashMap<>();

    public Transaction() {
        callingThread = Thread.currentThread();
        openTransactions.add(this);
    }

    void backup(Cluster<?> provenance, String recordId, Object nowValue) {
        if (!redactedInTransactionRecord.containsKey(provenance)) {
            redactedInTransactionRecord.put(provenance, new HashMap<>());
        }
        redactedInTransactionRecord.get(provenance).put(recordId, nowValue);
    }

    void rememberOfAdd(Cluster<?> provenance, String recordId) {
        if(!addedInTransactionRecord.containsKey(provenance)) {
            addedInTransactionRecord.put(provenance, new HashSet<>());
        }
        addedInTransactionRecord.get(provenance).add(recordId);
    }

    boolean recordWasLockedCurrentTransaction(Cluster<?> provenance, String recordId) {
        if(redactedInTransactionRecord.containsKey(provenance)) {
            return redactedInTransactionRecord.get(provenance).containsKey(recordId);
        } else return false;
    }

    public void commit() {
        redactedInTransactionRecord.forEach((cluster, stringObjectMap) -> cluster.commit(this, stringObjectMap.keySet()));
        redactedInTransactionRecord.clear();
    }

    public void rollback() {
        redactedInTransactionRecord.forEach(((cluster, stringObjectMap) ->
                cluster.rollback(stringObjectMap, addedInTransactionRecord.get(cluster))));
        redactedInTransactionRecord.clear();
    }

    protected boolean callingThreadIsDye() {
       return !callingThread.isAlive();
    }
}
