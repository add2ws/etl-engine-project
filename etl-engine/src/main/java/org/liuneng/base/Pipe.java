package org.liuneng.base;

import cn.hutool.core.util.IdUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.exception.DataflowException;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class Pipe {

    private Dataflow dataflowInstance;

    @Setter
    @Getter
    private String id;

    @Getter
    private long startTime = 0;

    private InputNode fromNode;

    private OutputNode toNode;

    @Getter
    private final int bufferCapacity;

    private final BlockingQueue<Row> bufferQueue;

    @Getter
    @Setter
    private boolean isValid;

    private boolean closed = false;

    public Pipe(int bufferCapacity) {
        this.id = "Pipe-" + IdUtil.fastSimpleUUID();
        this.bufferCapacity = bufferCapacity;
        bufferQueue = new ArrayBlockingQueue<>(this.bufferCapacity);
        isValid = true;
    }

    public Optional<InputNode> from() {
        return Optional.ofNullable(fromNode);
    }

    public void from(InputNode from) {
        this.fromNode = from;
        from.asNode().addPrevPipe(this);
    }

    public Optional<OutputNode> to() {
        return Optional.ofNullable(toNode);
    }

    public void to(OutputNode to) {
        this.toNode = to;
        to.asNode().setPrevPipe(this);
    }

    public void connect(InputNode inputNode, OutputNode outputNode) {
        this.from(inputNode);
        this.to(outputNode);
    }

    public int getCurrentBufferSize() {
        return bufferQueue.size();
    }

    public void beWritten(Row row) throws InterruptedException {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        if (!closed) {
            bufferQueue.put(row);
        }
    }

    public Row beRead() throws InterruptedException {
        if (!closed) {
            return bufferQueue.take();
        }
        throw new DataflowException("Pipe is closed");
    }

    protected Dataflow getDataflowInstance() {
        return dataflowInstance;
    }

    protected void setDataflowInstance(Dataflow dataflowInstance) {
        this.dataflowInstance = dataflowInstance;
    }

    protected void initialize(Dataflow dataFlow)  {
        this.dataflowInstance = dataFlow;
    }

    protected void stop() {
        closed = true;
        this.bufferQueue.drainTo(new ArrayList<>());
    }
}