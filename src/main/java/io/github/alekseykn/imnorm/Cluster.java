package io.github.alekseykn.imnorm;

import com.google.gson.Gson;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class Cluster<Record> {
    private boolean redacted = true;
    private final TreeMap<String, Record> data;

    Cluster(TreeMap<String, Record> map) {
        data = map;
    }

    Cluster(String id, Record record) {
        data = new TreeMap<>();
        data.put(id, record);
    }

    void set(String key, Record record) {
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

    Set<String> allKeys() {
        return data.keySet();
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
            if(counter++ > median) {
                newClusterData.put(entry.getKey(), entry.getValue());
                it.remove();
            }
        }
        return new Cluster<>(newClusterData);
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
}
