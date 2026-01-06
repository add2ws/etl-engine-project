package org.liuneng.base;

import lombok.NonNull;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeWritingException;

public interface OutputNode {

    void write(@NonNull Row row) throws NodeWritingException;

//    String[] getColumns() throws NodeException;

    String[] getColumns() throws NodeException;

    default Node asNode() {
        return (Node) this;
    }
}
