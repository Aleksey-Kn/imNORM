package io.github.alekseykn.imnorm;

import io.github.alekseykn.imnorm.exceptions.CreateDataStorageException;
import io.github.alekseykn.imnorm.exceptions.InternalImnormException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

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
    
    /**
     * Execute current queries, if this migration not executed earlier on this device.
     * If throw exception from migration, migration will rollback.
     *
     * @param migrationId Migration identifier, which determines whether the migration was performed earlier
     * @param migration   Calls to the data warehouse that will be performed as part of the migration
     * @return Exception, if migration throw exception. Optional.empty() if procedure completed correctly.
     */
    public Optional<Exception> executeMigration(String migrationId, BiConsumer<DataStorage, Transaction> migration) {
        try {
            if (executedMigrations.exists()) {
                if (Files.lines(executedMigrations.toPath()).noneMatch(line -> line.equals(migrationId))) {
                    return registerAndExecuteMigration(migrationId, migration);
                } else
                    return Optional.empty();
            } else {
                if (!executedMigrations.createNewFile())
                    throw new InternalImnormException("Create new file: " + executedMigrations.getPath());
                return registerAndExecuteMigration(migrationId, migration);
            }
        } catch (IOException e) {
            throw new InternalImnormException(e);
        }
    }

    /**
     * Execute migration and save its id.
     * If throw exception from migration, migration will rollback.
     *
     * @param migrationId Migration identifier, which determines whether the migration was performed earlier
     * @param migration   Calls to the data warehouse that will be performed as part of the migration
     * @return Exception, if migration throw exception. Optional.empty() if procedure completed correctly.
     * @throws IOException Exception with save migration id to file system
     */
    private Optional<Exception> registerAndExecuteMigration(String migrationId, BiConsumer<DataStorage,
            Transaction> migration) throws IOException {
        Optional<Exception> executeResult = Transaction
                .executeInWaitingTransactionWithReply(transaction -> migration.accept(this, transaction));

        if (executeResult.isEmpty()) {
            PrintWriter printWriter = new PrintWriter(new FileWriter(executedMigrations, true));
            printWriter.println(migrationId);
            printWriter.close();
        }
        return executeResult;
    }
}

