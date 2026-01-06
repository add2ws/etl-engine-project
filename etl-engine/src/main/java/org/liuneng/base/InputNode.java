package org.liuneng.base;

import lombok.NonNull;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeReadingException;

public interface InputNode {

    @NonNull
    Row read() throws NodeReadingException;

    String[] getColumns() throws NodeException;
//    String[] getColumns() throws NodeException;

    default Node asNode() {
        return (Node) this;
    }
}
