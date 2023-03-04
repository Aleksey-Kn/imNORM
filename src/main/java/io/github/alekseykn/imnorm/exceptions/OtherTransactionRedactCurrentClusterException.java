package io.github.alekseykn.imnorm.exceptions;

import io.github.alekseykn.imnorm.Cluster;

public class OtherTransactionRedactCurrentClusterException extends RuntimeException{
    public OtherTransactionRedactCurrentClusterException(String firstClusterKey) {
        super("Multiple access to cluster " + firstClusterKey);
    }
}
