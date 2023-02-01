package ru.imnormproject.imnorm;

import ru.imnormproject.imnorm.repositories.Repository;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DataStorage {
    private static final Map<Path, DataStorage> createdDataStorage = new HashMap<>();

    public static DataStorage getDataStorage(Path path) {
        path = path.toAbsolutePath();
        if(!createdDataStorage.containsKey(path)) {
            createdDataStorage.put(path, new DataStorage(path));
        }
        return createdDataStorage.get(path);
    }


    private final Path nowPath;
    private final Map<Class, Repository> createdRepository = new HashMap<>();

    private DataStorage(Path path) {
        nowPath = path;
    }
}
