package io.github.alekseykn.imnorm.exceptions;

public class InternalImnormException extends RuntimeException {
    public InternalImnormException(Exception cause) {
        super(cause);
    }
    
    public InternalImnormException(String cause) {
        super("Cannot execute command: " + cause);
    }
}
