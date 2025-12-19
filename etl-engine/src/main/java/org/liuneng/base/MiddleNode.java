package org.liuneng.base;

import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeReadingException;
import org.liuneng.exception.NodeWritingException;
import org.liuneng.util.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public abstract class MiddleNode extends Node implements InputNode, OutputNode {
    private final BlockingQueue<Row> list = new SynchronousQueue<>();

    @Override
    public Row read() throws NodeReadingException {
        try {
            return list.take();
        } catch (InterruptedException e) {
            throw new NodeReadingException(e);
        }
    }

    @Override
    public void write(Row row) throws NodeWritingException {
        row = process(row);
        try {
            list.put(row);
        } catch (InterruptedException e) {
            throw new NodeWritingException(e);
        }
    }

    public abstract Row process(Row row);

    public List<Tuple2<String, String>> getColumnMapping() {
        return Collections.emptyList();
    }

    @Override
    public String[] getInputColumns() throws NodeException {
        return this.getPreviousNode().orElseThrow(() -> new NodeException("无法获得上个节点的列")).getInputColumns();
    }

    @Override
    public Node asNode() {
        return this;
    }
}
