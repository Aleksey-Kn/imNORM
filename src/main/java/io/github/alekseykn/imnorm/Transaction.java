package io.github.alekseykn.imnorm;

import java.util.HashMap;
import java.util.Map;

public class Transaction {
    private final Map<Repository<?>, Map<String, Object>> blockedRecord = new HashMap<>();

    protected void lock(Repository<?> provenance, String recordId, Object nowValue) {
        if (!blockedRecord.containsKey(provenance)) {
            blockedRecord.put(provenance, new HashMap<>());
        }
        blockedRecord.get(provenance).put(recordId, nowValue);
    }

    protected boolean recordWasLocked(Repository<?> provenance, String recordId) {
        if(blockedRecord.containsKey(provenance)) {
            return blockedRecord.get(provenance).containsKey(recordId);
        } else return false;
    }

    public void commit() {
        blockedRecord.forEach((repository, stringObjectMap) -> repository.unlock(stringObjectMap.keySet()));
    }

    public void rollback() {
        blockedRecord.forEach(Repository::rollback);
    }
}
