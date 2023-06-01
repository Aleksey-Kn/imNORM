package io.github.alekseykn.imnorm.exceptions;

public class DeadLockException extends RuntimeException {
    public DeadLockException(int firstClusterKey) {
        super("Multiple access to cluster " + firstClusterKey);
    }
}
