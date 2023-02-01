package ru.imnormproject.imnorm.exceptions;

public class CountIdException extends RuntimeException{
    public CountIdException(Class clas) {
        super(String.format("Entity %s must contain just one 'id' field", clas.getName()));
    }
}
