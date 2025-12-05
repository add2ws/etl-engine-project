package org.liuneng.base;

import lombok.Data;

@Data
public class EtlLog {

    private String id;

    private String dataflowID;

    private String nodeID;

    private long inputted;

    private long outputted;

    private long inserted;

    private long updated;

    private int currentBufferSize;

    private long timestamp;

    private LogLevel logLevel;

    private String message;


    public static EtlLog errorLog(String message) {
        EtlLog etlLog = new EtlLog();
        etlLog.setLogLevel(LogLevel.ERROR);
        etlLog.setMessage(message);
        return etlLog;
    }

    public static EtlLog infoLog(String message) {
        EtlLog etlLog = new EtlLog();
        etlLog.setLogLevel(LogLevel.INFO);
        etlLog.setMessage(message);
        return etlLog;
    }

}
