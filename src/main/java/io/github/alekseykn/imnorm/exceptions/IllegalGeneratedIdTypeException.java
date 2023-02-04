package io.github.alekseykn.imnorm.exceptions;

public class IllegalGeneratedIdTypeException extends RuntimeException {
    public IllegalGeneratedIdTypeException() {
        super("Generated id must be numeric or string type");
    }
}
