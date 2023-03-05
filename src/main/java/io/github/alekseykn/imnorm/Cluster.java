package io.github.alekseykn.imnorm;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.exceptions.MultipleAccessToCluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public final class Cluster<Record> {
    private final Repository<Record> repository;
    private TreeMap<String, Record> copyDataForTransactions = null;
    private boolean redacted = true;
    private TreeMap<String, Record> data = new TreeMap<>();
    private final String firstKey;

    Cluster(TreeMap<String, Record> map, Repository<Record> owner) {
        data = map;
        repository = owner;
        firstKey = data.firstKey();
    }

    Cluster(String id, Record record, Repository<Record> owner) {
        data.put(id, record);
        repository = owner;
        firstKey = id;
    }

    Cluster(String id, Record record, Repository<Record> owner, Transaction transaction) {
        copyDataForTransactions = new TreeMap<>();
        copyDataForTransactions.put(id, record);
        repository = owner;
        firstKey = id;

        transaction.captureLock(this);
    }

    void set(String key, Record record) {
        if(Objects.nonNull(copyDataForTransactions))
            throw new MultipleAccessToCluster(firstKey);
        redacted = true;
        data.put(key, record);
    }

    void set(String key, Record record, Transaction transaction) {
        lock(transaction);
        copyDataForTransactions.put(key, record);
    }

    Record get(String key) {
        if(Objects.nonNull(copyDataForTransactions))
            throw new MultipleAccessToCluster(firstKey);
        return data.get(key);
    }

    Record get(String key, Transaction transaction) {
        lock(transaction);
        return copyDataForTransactions.get(key);
    }

    Collection<Record> findAll() {
        if(Objects.nonNull(copyDataForTransactions))
            throw new MultipleAccessToCluster(firstKey);
        return data.values();
    }

    Collection<Record> findAll(Transaction transaction) {
        lock(transaction);
        return copyDataForTransactions.values();
    }

    Record delete(String key) {
        if(Objects.nonNull(copyDataForTransactions))
            throw new MultipleAccessToCluster(firstKey);
        redacted = true;
        return data.remove(key);
    }

    Record delete(String key, Transaction transaction) {
        lock(transaction);
        return copyDataForTransactions.remove(key);
    }

    int size() {
        return data.size();
    }

    boolean containsKey(String key) {
        return data.containsKey(key);
    }

    boolean containsKeyFromTransaction(String key) {
        return Objects.nonNull(copyDataForTransactions)
                ? copyDataForTransactions.containsKey(key)
                : data.containsKey(key);
    }

    String getFirstKey() {
        return firstKey;
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

    private void lock(Transaction transaction) {
        if (!transaction.lockOwner(this)) {
            if (Objects.isNull(copyDataForTransactions)) {
                copyDataForTransactions = new TreeMap<>(data);
            } else {
                transaction.rollback();
                throw new MultipleAccessToCluster(firstKey);
            }
            transaction.captureLock(this);
        }
    }

    void commit() {
        if (Objects.nonNull(copyDataForTransactions)) {
            data = copyDataForTransactions;
            copyDataForTransactions = null;
            redacted = true;
        }
    }

    void rollback() {
        copyDataForTransactions = null;
    }

    boolean hasNotOpenTransactions() {
        return Objects.isNull(copyDataForTransactions);
    }
}
