package io.github.alekseykn.imnorm;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Anyone methods is synchronized, because in them re-assigned 'openedCluster'.
 *
 * @param <Record> Type of entity for this repository
 */
public final class FrugalRepository<Record> extends Repository<Record> {
    private final TreeSet<String> clusterNames = new TreeSet<>();
    private Cluster<Record> openCluster;
    private String openClusterName;

    FrugalRepository(Class<Record> type, File directory) {
        super(type, directory);
    }

    @Override
    protected synchronized Cluster<Record> findCurrentCluster(String id) {
        if (Objects.nonNull(openCluster) && openCluster.containsKey(id)) {
            return openCluster;
        } else {
            Record now;
            flush();
            synchronized (clusterNames) {
                openClusterName = clusterNames.floor(id);
            }
            try (Scanner scanner = new Scanner(new File(directory.getAbsolutePath(), openClusterName))) {
                ConcurrentHashMap<String, Record> tempClusterData = new ConcurrentHashMap<>();
                while (scanner.hasNextLine()) {
                    now = gson.fromJson(scanner.nextLine(), type);
                    tempClusterData.put(getStringIdFromRecord.apply(now), now);
                }
                openCluster = new Cluster<>(tempClusterData);
                return openCluster;
            } catch (FileNotFoundException e) {
                return null;
            }
        }
    }

    @Override
    protected Record create(String id, Record record) {
        return null;
    }

    @Override
    public Set<Record> findAll() {
        waitAllRecord();
        synchronized (clusterNames) {
            return clusterNames.stream()
                    .flatMap(clusterName -> {
                        try {
                            return Files.lines(Path.of(directory.getAbsolutePath(), clusterName));
                        } catch (IOException e) {
                            throw new InternalImnormException(e);
                        }
                    })
                    .map(record -> gson.fromJson(record, type))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public Set<Record> findAll(int startIndex, int rowCount) {
        HashSet<Record> result = new HashSet<>(rowCount);
        List<Record> afterSkippedClusterValues;
        Stream<Record> clusterRecords;
        try {
            synchronized (clusterNames) {
                for (String clusterName : clusterNames) {
                    clusterRecords = Files.lines(Path.of(directory.getAbsolutePath(), clusterName))
                            .map(s -> gson.fromJson(s, type));
                    if (clusterRecords.count() < startIndex) {
                        startIndex -= clusterRecords.count();
                    } else {
                        afterSkippedClusterValues = clusterRecords
                                .sorted(Comparator.comparing(getStringIdFromRecord))
                                .skip(startIndex)
                                .limit(rowCount)
                                .collect(Collectors.toList());
                        //todo
                        afterSkippedClusterValues
                                .forEach(record -> this.waitRecordForTransactions(getStringIdFromRecord.apply(record)));
                        result.addAll(afterSkippedClusterValues);

                        rowCount -= afterSkippedClusterValues.size();
                        startIndex = 0;
                    }
                    if (rowCount == 0)
                        break;
                }
                return result;
            }
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
    }

    @Override
    public synchronized void flush() {
        //todo
        waitAllRecord();
        synchronized (clusterNames) {
            if (needGenerateId) {
                try (DataOutputStream outputStream = new DataOutputStream(
                        new FileOutputStream(new File(directory.getAbsolutePath(), "_sequence.imnorm")))) {
                    outputStream.writeLong(sequence);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (openCluster.isRedacted()) {
                try (PrintWriter printWriter = new PrintWriter(new File(directory.getAbsolutePath(), openClusterName))) {
                    openCluster.findAll().forEach(record -> printWriter.println(gson.toJson(record)));
                    openCluster.wasFlush();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
