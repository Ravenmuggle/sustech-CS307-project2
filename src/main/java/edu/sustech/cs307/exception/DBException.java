package edu.sustech.cs307.exception;

public class DBException extends Exception {
    private final ExceptionTypes exceptionType;
    public DBException(ExceptionTypes exceptionType) {
        super(String.format(
                "%s: %s", exceptionType.name(), exceptionType.GetErrorResult()
        ));
        this.exceptionType=exceptionType;
    }
    public ExceptionTypes getType(){
        return this.exceptionType;
    }
}
