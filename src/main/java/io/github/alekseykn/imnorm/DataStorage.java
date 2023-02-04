package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.CreateDataStorageException;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DataStorage {
    private static final Map<Path, DataStorage> createdDataStorage = new HashMap<>();

    public static DataStorage getDataStorage() {
        return getDataStorage(Path.of("data"));
    }

    public static DataStorage getDataStorage(Path path) {
        path = path.toAbsolutePath();
        File rootDataStorageDirectory = path.toFile();
        if(!rootDataStorageDirectory.exists()) {
            if(!rootDataStorageDirectory.mkdir())
                throw new CreateDataStorageException(rootDataStorageDirectory);
        }

        if(!createdDataStorage.containsKey(path)) {
            createdDataStorage.put(path, new DataStorage(path));
        }
        return createdDataStorage.get(path);
    }


    private final Path nowPath;
    private final Map<Class<?>, Repository<?>> createdRepository = new HashMap<>();

    private DataStorage(Path path) {
        nowPath = path;
    }

    public <Value> Repository<Value> getFastRepositoryForClass(Class<Value> clas) {
        if(!createdRepository.containsKey(clas)) {
            createdRepository.put(clas,
                    new FastRepository<>(clas,
                            Path.of(nowPath.toString(), clas.getName().replace('.', '_')).toFile()));
        }
        return (Repository<Value>) createdRepository.get(clas);
    }
}
