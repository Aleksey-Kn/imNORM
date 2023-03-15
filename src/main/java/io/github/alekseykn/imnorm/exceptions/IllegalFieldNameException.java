package io.github.alekseykn.imnorm.exceptions;

public class IllegalFieldNameException extends RuntimeException {
    public IllegalFieldNameException(String fieldName, Class<?> clas) {
        super("Not fount field '%s' from class '%s'".formatted(fieldName, clas.getName()));
    }
}
