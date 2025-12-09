package org.liuneng.base;

import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeReadingException;

public interface InputNode {

    long getProcessed();

    long getProcessingRate();

    long getStartTime();

    Row read() throws NodeReadingException;

    String[] getInputColumns() throws NodeException;

    default Node asNode() {
        return (Node) this;
    }
}
