package org.liuneng.base;

public interface InputNode {

    long getProcessed();

    long getProcessingRate();

    long getStartTime();

    Row read() throws Exception;

    String[] getInputColumns() throws Exception;

    default Node asNode() {
        return (Node) this;
    }
}
