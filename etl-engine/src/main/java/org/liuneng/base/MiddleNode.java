package org.liuneng.base;

import lombok.NonNull;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeReadingException;
import org.liuneng.exception.NodeWritingException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public abstract class MiddleNode extends Node implements InputNode, OutputNode {

    public enum Type {
        COPY, SWITCH
    }

    public enum FailedPolicy {
        TERMINATE_DATAFLOW,
        END_DOWNSTREAM
    }


    private final BlockingQueue<Row> list = new SynchronousQueue<>();


    @Override
    @NonNull
    public Row read() throws NodeReadingException {
        try {
            return list.take();
        } catch (InterruptedException e) {
            throw new NodeReadingException(e);
        }
    }

    @Override
    public void write(@NonNull Row row) throws NodeWritingException {
        row = process(row);
        try {
            list.put(row);
        } catch (InterruptedException e) {
            throw new NodeWritingException(e);
        }
    }

    @Override
    public String[] getInputColumns() throws NodeException {
        return new String[0];
    }

    @Override
    public Node asNode() {
        return InputNode.super.asNode();
    }

    @Override
    public String[] getOutputColumns() throws NodeException {
        return new String[0];
    }

    @NonNull
    protected abstract Row process(@NonNull Row row) throws NodeException;

    @NonNull
    public abstract Type getType();

    public FailedPolicy getFailedPolicy() {
        return FailedPolicy.TERMINATE_DATAFLOW;
    }

}