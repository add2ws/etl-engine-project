package org.liuneng.base;

import org.liuneng.exception.NodeWritingException;

public interface OutputNode {

    long getProcessed();

    long getProcessingRate();

    long getStartTime();

    void write(Row row) throws NodeWritingException;

    String[] getOutputColumns() throws Exception;

    default Node asNode() {
        return (Node) this;
    }
}
