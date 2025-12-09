package org.liuneng.exception;

public class DataflowException extends EtlEngineException {
    public DataflowException(String message) {
        super(message);
    }

    public DataflowException(Throwable cause) {
        super(cause);
    }
}
