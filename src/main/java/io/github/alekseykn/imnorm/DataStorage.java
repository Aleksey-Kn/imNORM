package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.CreateDataStorageException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Container for repository. Need for create new repository and determines their location in the file system.
 *
 * @author Aleksey-Kn
 */
public class DataStorage {
    /**
     * Collection instances of DataStorage. Used to maintain a single connection to each data storage,
     * for avoiding simultaneous access to data from different streams
     */
    private static final Map<Path, DataStorage> createdDataStorage = new HashMap<>();


    /**
     * Create instances of DataStorage with standard path
     *
     * @return Instances of DataStorage with standard path
     */
    public synchronized static DataStorage getDataStorage() {
        return getDataStorage(Path.of("data"));
    }

    /**
     * Create instances of DataStorage with specified path
     *
     * @param path The path where the data storage will be located
     * @return Instances of DataStorage with specified path
     */
    public synchronized static DataStorage getDataStorage(Path path) {
        path = path.toAbsolutePath();
        File rootDataStorageDirectory = path.toFile();
        if (!rootDataStorageDirectory.exists()) {
            if (!rootDataStorageDirectory.mkdirs())
                throw new CreateDataStorageException(rootDataStorageDirectory);
        }

        if (!createdDataStorage.containsKey(path)) {
            createdDataStorage.put(path, new DataStorage(path));
        }
        return createdDataStorage.get(path);
    }

    /**
     * Absolute path to current data storage
     */
    private final Path nowPath;

    /**
     * Collection repository, contains in current repository
     */
    private final Map<Class<?>, Repository<?>> createdRepository = new HashMap<>();

    private DataStorage(Path path) {
        nowPath = path;
    }

    /**
     * Create fast repository or return exists, if it was created earlier.
     * If exists repository have other type, return repository other type instead of the requested.
     *
     * @param clas    Class of entity
     * @param <Value> Type of entity
     * @return Repository for work with current entity
     */
    public synchronized <Value> Repository<Value> getPreferablyFastRepositoryForClass(Class<Value> clas) {
        if (!createdRepository.containsKey(clas)) {
            createdRepository.put(clas, new FastRepository<>(clas, directoryForRepository(clas)));
        }
        return (Repository<Value>) createdRepository.get(clas);
    }

    /**
     * Create frugal repository or return exists, if it was created earlier.
     * If exists repository have other type, return repository other type instead of the requested
     *
     * @param clas    Class of entity
     * @param <Value> Type of entity
     * @return Repository for work with current entity
     */
    public synchronized <Value> Repository<Value> getPreferablyFrugalRepositoryForClass(Class<Value> clas, int repositoryMaxMegabyteSize) {
        if (!createdRepository.containsKey(clas)) {
            createdRepository.put(clas, new FrugalRepository<>(clas, directoryForRepository(clas),
                    repositoryMaxMegabyteSize * 100));
        }
        return (Repository<Value>) createdRepository.get(clas);
    }

    /**
     * Create fast repository or return exists, if it was created earlier.
     * If exists repository have other type,
     * makes an existing repository unavailable for writing and creates a new repository of the specified type
     *
     * @param clas    Class of entity
     * @param <Value> Type of entity
     * @return Repository for work with current entity
     */
    public synchronized <Value> Repository<Value> getStrictlyFastRepositoryForClass(Class<Value> clas) {
        if (createdRepository.containsKey(clas)) {
            if (createdRepository.get(clas) instanceof FastRepository) {
                return (Repository<Value>) createdRepository.get(clas);
            } else {
                Repository<?> oldRepository = createdRepository.get(clas);
                oldRepository.flush();
                oldRepository.lock();
            }
        }
        Repository<Value> repository = new FastRepository<>(clas, directoryForRepository(clas));
        createdRepository.put(clas, repository);
        return repository;
    }

    /**
     * Create frugal repository or return exists, if it was created earlier.
     * If exists repository have other type,
     * makes an existing repository unavailable for writing and creates a new repository of the specified type
     *
     * @param clas    Class of entity
     * @param <Value> Type of entity
     * @return Repository for work with current entity
     */
    public synchronized <Value> Repository<Value> getStrictlyFrugalRepositoryForClass(Class<Value> clas,
                                                                                      int repositoryMaxMegabyteSize) {
        if (createdRepository.containsKey(clas)) {
            if (createdRepository.get(clas) instanceof FrugalRepository) {
                return (Repository<Value>) createdRepository.get(clas);
            } else {
                Repository<?> oldRepository = createdRepository.get(clas);
                oldRepository.flush();
                oldRepository.lock();
            }
        }
        Repository<Value> repository = new FrugalRepository<>(clas, directoryForRepository(clas),
                repositoryMaxMegabyteSize * 100);
        createdRepository.put(clas, repository);
        return repository;
    }

    /**
     * Create repository or return exists, if it was created earlier.
     * The memory allocated for the repository is calculated automatically based on the free memory on the device.
     *
     * @param clas    Class of entity
     * @param <Value> Type of entity
     * @return Repository for work with current entity
     */
    public synchronized <Value> Repository<Value> getRepositoryForClass(Class<Value> clas) {
        if (!createdRepository.containsKey(clas)) {
            File repositoryDirectory = directoryForRepository(clas);
            if (repositoryDirectory.exists()) {
                createdRepository.put(clas, getPreferablyFrugalRepositoryForClass(clas,
                        (int) ((Runtime.getRuntime().maxMemory() - usedMemory()) / 10_485_760)));
            } else {
                createdRepository.put(clas, getPreferablyFastRepositoryForClass(clas));
            }
        }
        return (Repository<Value>) createdRepository.get(clas);
    }

    /**
     * Create directory name for repository from entity class name
     *
     * @param forClass Entity class type
     * @return Directory for repository
     */
    private File directoryForRepository(Class<?> forClass) {
        return Path.of(nowPath.toString(), forClass.getName().replace('.', '_')).toFile();
    }

    /**
     * Calculate memory used by the program
     *
     * @return Memory used by the program in bytes
     */
    private long usedMemory() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }
}

