package io.github.alekseykn.imnorm.exceptions;

public class TransactionWasCloseException extends RuntimeException {
    public TransactionWasCloseException() {
        super("Transaction can't use after 'commit' or 'rollback'");
    }
}
