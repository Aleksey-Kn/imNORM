package io.github.alekseykn.imnorm;

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
    private final Map<Class<?>, Repository<?>> createdRepository = new HashMap<>();

    private DataStorage(Path path) {
        nowPath = path;
    }

    public <Value> Repository<Value> getFastRepositoryForClass(Class<Value> clas) {
        if(!createdRepository.containsKey(clas)) {
            createdRepository.put(clas,
                    new FastRepository<>(clas, Path.of(nowPath.toString(), clas.getName()).toFile()));
        }
        return (Repository<Value>) createdRepository.get(clas);
    }
}
