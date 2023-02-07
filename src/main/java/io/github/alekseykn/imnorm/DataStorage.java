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

    private <Value> Repository<Value> getRepository(Class<Value> clas,
                                                    BiFunction<Class<Value>, File, Repository<Value>> constructor) {
        if (!createdRepository.containsKey(clas)) {
            createdRepository.put(clas, constructor.apply(clas, directoryForRepository(clas)));
        }
        return (Repository<Value>) createdRepository.get(clas);
    }

    public <Value> Repository<Value> getFastRepositoryForClass(Class<Value> clas) {
        return getRepository(clas, FastRepository::new);
    }

    public <Value> Repository<Value> getFrugalRepositoryForClass(Class<Value> clas) {
        return getRepository(clas, FrugalRepository::new);
    }

    public <Value> Repository<Value> getRepositoryForClass(Class<Value> clas) {
        File repositoryDirectory = directoryForRepository(clas);
        if (repositoryDirectory.exists()) {
            return (Runtime.getRuntime().maxMemory() - usedMemory()) / 2 >
                    Objects.requireNonNull(repositoryDirectory.list()).length * 100_000L
                    ? getFastRepositoryForClass(clas)
                    : getFrugalRepositoryForClass(clas);
        } else {
            return getFastRepositoryForClass(clas);
        }
    }

    private File directoryForRepository(Class<?> forClass) {
        return Path.of(nowPath.toString(), forClass.getName().replace('.', '_')).toFile();
    }

    private long usedMemory() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }
}
