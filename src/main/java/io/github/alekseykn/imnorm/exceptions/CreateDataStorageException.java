package io.github.alekseykn.imnorm.exceptions;

import java.io.File;

public class CreateDataStorageException extends RuntimeException {
    public CreateDataStorageException(File directory) {
        super("Can't create directory " +  directory.getPath());
    }
}
