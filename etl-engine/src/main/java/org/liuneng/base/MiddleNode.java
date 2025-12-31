package org.liuneng.base;

import lombok.NonNull;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeReadingException;
import org.liuneng.exception.NodeWritingException;

public interface MiddleNode extends InputNode, OutputNode {

    enum Type {
        COPY, SWITCH
    }

    enum FailedPolicy {
        TERMINATE_DATAFLOW,
        END_DOWNSTREAM
    }

    @NonNull
    Row process(@NonNull Row row) throws NodeException;

    default Type getType() {
        return Type.COPY;
    }

    default FailedPolicy getFailedPolicy() {
        return FailedPolicy.TERMINATE_DATAFLOW;
    }

    default Node asNode() {
        return (Node) this;
    }

    @Override
    default void write(Row row) throws NodeWritingException {

    }

    @NonNull
    @Override
    default Row read() throws NodeReadingException {
        return null;
    }

    @Override
    default String[] getInputColumns() throws NodeException {
        return null;
    }

    @Override
    default String[] getOutputColumns() throws NodeException {
        return null;
    }
}