package org.liuneng.base;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeReadingException;
import org.liuneng.exception.NodeWritingException;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

@Slf4j
public abstract class MiddleNode extends Node implements InputNode, OutputNode {

    private boolean isClosed = false;

    public enum Type {
        COPY, SWITCH
    }

    public enum FailedPolicy {
        TERMINATE_DATAFLOW,
        END_DOWNSTREAM
    }


    private final BlockingQueue<Row> blockingQueue = new SynchronousQueue<>();

    @Override
    public void write(@NonNull Row row) throws NodeWritingException {
        if (isClosed) {
            return;
        }
        try {
            row = process(row);
//            String id = IdUtil.nanoId(5);
//            log.info("开始写入_NO[{}]" , id);
            blockingQueue.put(row);
//            log.info("完成写入_NO[{}]" , id);
        } catch (InterruptedException | NodeException e) {
            throw new NodeWritingException(e);
        }
    }

    @Override
    @NonNull
    public Row read() throws NodeReadingException {
        if (isClosed) {
            throw new NodeReadingException("Node is closed");
        }

        try {
//            String id = IdUtil.nanoId(5);
//            log.info("开始读取_NO[{}]" , id);
            Row taken = blockingQueue.take();
//            log.info("完成读取_NO[{}]" , id);
            return taken;
        } catch (InterruptedException e) {
            throw new NodeReadingException(e);
        }
    }

    @Override
    public Node asNode() {
        return InputNode.super.asNode();
    }

    @NonNull
    protected abstract Row process(@NonNull Row row) throws NodeException;

    @NonNull
    public abstract Type getType();

    public FailedPolicy getFailedPolicy() {
        return FailedPolicy.TERMINATE_DATAFLOW;
    }

    @Override
    protected void onDataflowStop() {
        isClosed = true;
        this.blockingQueue.drainTo(new ArrayList<>());
    }
}