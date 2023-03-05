package io.github.alekseykn.imnorm.exceptions;

public class MultipleAccessToCluster extends RuntimeException{
    public MultipleAccessToCluster(String firstClusterKey) {
        super("Multiple access to cluster " + firstClusterKey);
    }
}
