package io.github.alekseykn.imnorm.utils;

import com.google.gson.Gson;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * A tool for mapping clusters to a file system and unloading clusters from it
 *
 * @param <Record> Cluster record type
 * @author Aleksey-Kn
 */
public class ClusterFileManipulator<Record> {
    private final Gson gson = new Gson();
    private final Class<Record> type;
    private final Field id;

    public ClusterFileManipulator(Class<Record> recordType, Field idField) {
        type = recordType;
        id = idField;
    }

    /**
     * Loads cluster data from the file system
     *
     * @param clusterPath Address of the cluster file in the file system
     * @return Indexed collection of records
     */
    public TreeMap<Integer, Map<Object, Record>> read(final Path clusterPath) {
        try (Stream<String> lines = Files.lines(clusterPath)) {
            TreeMap<Integer, Map<Object, Record>> tempClusterData = new TreeMap<>();
            lines.forEach(line -> {
                int index = line.indexOf(':');
                tempClusterData.put(Integer.parseInt(line.substring(0, index)),
                        splitRecordsFromCurrentHash(line.substring(index + 1)));
            });
            return tempClusterData;
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
    }

    /**
     * Splits and collects records with the same hash
     *
     * @param data String to be split
     * @return Indexed collection of records with the same hash
     */
    private Map<Object, Record> splitRecordsFromCurrentHash(final String data) {
        final HashMap<Object, Record> result = new HashMap<>();
        Record record;

        try {
            for (int nowIndex = 0, startIndex = 0, counter = 0; nowIndex < data.length(); nowIndex++) {
                switch (data.charAt(nowIndex)) {
                    case '{' -> counter++;
                    case '}' -> counter--;
                    case '#' -> {
                        if (counter == 0) {
                            record = gson.fromJson(data.substring(startIndex, nowIndex), type);
                            result.put(id.get(record), record);

                            startIndex = nowIndex + 1;
                        }
                    }
                }
            }
            return result;
        } catch (IllegalAccessException e) {
            throw new InternalImnormException(e);
        }
    }

    /**
     * Write cluster data to file system with the necessary markup
     *
     * @param clusterFile Address in file system for write data
     * @param data        Cluster records
     */
    public void write(final File clusterFile, final TreeMap<Integer, Map<Object, Record>> data) {
        try (PrintWriter printWriter = new PrintWriter(clusterFile)) {
            data.forEach((hash, values) -> {
                printWriter.print(hash + ":");
                values.values().forEach(record -> printWriter.print(gson.toJson(record).concat("#")));
                printWriter.println();
            });
        } catch (FileNotFoundException e) {
            throw new InternalImnormException(e);
        }
    }
}
