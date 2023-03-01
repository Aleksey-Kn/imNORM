package io.github.alekseykn.imnorm;

import com.google.gson.Gson;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class Cluster<Record> {
    private final Set<String> blockingId = ConcurrentHashMap.newKeySet();
    private final Repository<Record> repository;
    private boolean redacted = true;
    private final TreeMap<String, Record> data;

    Cluster(TreeMap<String, Record> map, Repository<Record> owner) {
        data = map;
        repository = owner;
    }

    Cluster(String id, Record record, Repository<Record> owner) {
        data = new TreeMap<>();
        data.put(id, record);
        repository = owner;
    }

    void set(String key, Record record) {
        waitAndLock(key);
        redacted = true;
        data.put(key, record);
    }

    Record get(String key) {
        return data.get(key);
    }

    Collection<Record> findAll() {
        return data.values();
    }

    Record delete(String key) {
        waitAndLock(key);
        redacted = true;
        return data.remove(key);
    }

    int size() {
        return data.size();
    }

    boolean containsKey(String key) {
        return data.containsKey(key);
    }

    String firstKey() {
        return data.firstKey();
    }

    boolean isEmpty() {
        return data.isEmpty();
    }

    Cluster<Record> split() {
        TreeMap<String, Record> newClusterData = new TreeMap<>();
        int counter = 0;
        final int median = data.size() / 2;
        Iterator<Map.Entry<String, Record>> it = data.entrySet().iterator();
        Map.Entry<String, Record> entry;
        while (it.hasNext()) {
            entry = it.next();
            if (counter++ > median) {
                newClusterData.put(entry.getKey(), entry.getValue());
                it.remove();
            }
        }
        return new Cluster<>(newClusterData, repository);
    }

    void flush(File toFile, Gson parser) {
        if (redacted) {
            try (PrintWriter printWriter = new PrintWriter(toFile)) {
                findAll().forEach(record -> printWriter.println(parser.toJson(record)));
                redacted = false;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void waitAndLock(String id) {
        try {
            while (blockingId.contains(id)) {
                repository.wait();
            }
            blockingId.add(id);
        } catch (InterruptedException ignore) {
        }
    }

    void unlock(Set<String> identities) {
        blockingId.removeAll(identities);
        repository.notifyAll();
    }

    void rollback(Map<String, Object> rollbackRecord) {
        rollbackRecord.forEach((id, record) -> set(id, (Record) record));
        unlock(rollbackRecord.keySet());
    }
}
