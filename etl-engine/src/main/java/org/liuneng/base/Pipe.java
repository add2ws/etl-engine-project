package org.liuneng.base;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Pipe {
    final static Logger log = LoggerFactory.getLogger(Dataflow.class);

    private Dataflow dataflowInstance;

    @Setter
    @Getter
    private String id;

    @Getter
    private long startTime = 0;

    private InputNode from;

    private OutputNode to;

    private final int bufferCapacity;

    private final BlockingQueue<Row> bufferQueue;

    private boolean isValid;

    private boolean closed = false;

    public Pipe(int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
        bufferQueue = new ArrayBlockingQueue<>(this.bufferCapacity);
        isValid = true;
    }

    public Optional<InputNode> getFrom() {
        return Optional.ofNullable(from);
    }

    public void setFrom(InputNode from) {
        this.from = from;
        from.asNode().addAfterPipe(this);
    }

    public Optional<OutputNode> getTo() {
        return Optional.ofNullable(to);
    }

    public void setTo(OutputNode to) {
        this.to = to;
        to.asNode().setBeforePipe(this);
    }

    public void connect(InputNode inputNode, OutputNode outputNode) {
        this.setFrom(inputNode);
        this.setTo(outputNode);
    }

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    public int getCurrentBufferSize() {
        return bufferQueue.size();
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public void write(Row row) throws InterruptedException {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        if (!closed) {
            bufferQueue.put(row);
        }
    }

    public Row read() throws InterruptedException {
        return bufferQueue.take();
    }

    protected Dataflow getDataflowInstance() {
        return dataflowInstance;
    }

    protected void setDataflowInstance(Dataflow dataflowInstance) {
        this.dataflowInstance = dataflowInstance;
    }

    protected void initialize(Dataflow dataFlow)  {

        this.dataflowInstance = dataFlow;
//        this.dataflowInstance.getDataTransferExecutor().execute(() -> {
//            try {
//                dataflowInstance.awaitStoppingSignal();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            } finally {
//                bufferQueue.drainTo(new ArrayList<>());
//            }
//        });
    }

    protected void stop() {
        closed = true;
        bufferQueue.clear();
//        bufferQueue.drainTo(new ArrayList<>());
    }
}
