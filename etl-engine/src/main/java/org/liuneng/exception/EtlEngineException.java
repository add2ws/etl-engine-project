package org.liuneng.exception;

public class EtlEngineException extends RuntimeException {
    public EtlEngineException(String message) {
        super(message);
    }

    public EtlEngineException(Throwable cause) {
        super(cause);
    }
}
