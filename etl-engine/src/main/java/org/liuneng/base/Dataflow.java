package org.liuneng.base;

import cn.hutool.core.util.IdUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.exception.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Dataflow {
    public enum Status {
        IDLE, RUNNING, STOPPING, STOPPED
    }

    @Getter
    @Setter
    private String id;

    @Getter
    private Status status;

    @Getter
    private final Node head;

    @Getter
    @Setter
    private int maxRetryCount = 3;

    @Getter
    @Setter
    private int retryTimeoutSeconds = 3;

    @Getter
    @Setter
    private int processingThresholdLog = 20000;

    private final AtomicInteger allCompetedOutputCount = new AtomicInteger(0);

    private final ExecutorService dataTransferThreadPool;

    @Getter
    private long startTime;

    @Getter
    private long endTime;

    @Getter
    private final List<EtlLog> logList = new ArrayList<>();

    @Getter
    @Setter
    private boolean printLogToConsole = true;



    public Dataflow(Node headNode) {
        this.id = "Dataflow-" + IdUtil.fastSimpleUUID();
        this.head = headNode;
        this.status = Status.IDLE;
        this.dataTransferThreadPool = Executors.newCachedThreadPool(r -> new Thread(r, "DataTransferThread-" + this.id));
    }

    protected void writeInfoLog(String message) {
        writeLogOfNode(null, LogLevel.INFO, message);
    }

    protected void writeErrorLog(String message, Throwable e) {
        writeLogOfNode(null, LogLevel.ERROR, message, e);
    }

    protected void writeErrorLog(String message) {
        writeLogOfNode(null, LogLevel.ERROR, message);
    }

    protected void writeLogOfNode(Node node, LogLevel logLevel, String message, Throwable e) {
        EtlLog etlLog = new EtlLog();
        etlLog.setId(IdUtil.fastSimpleUUID());
        etlLog.setLogLevel(logLevel);
        etlLog.setMessage(message);
        etlLog.setTimestamp(System.currentTimeMillis());
        if (node != null) {
            etlLog.setNodeID(node.getId());
        }
        logList.add(etlLog);

        if (!printLogToConsole) {
            return;
        }

        if (logLevel == LogLevel.INFO) {
            if (node == null) {
                log.info("Log message from Dataflow[ID={}]:{}", this.getId(), message);
            } else {
                log.info("Log message from Node[Name={}]: {}", node.getName(), message);
            }
        } else if (logLevel == LogLevel.ERROR) {
            if (node == null) {
                if (e != null) {
                    log.error("Error message from Dataflow[ID={}]:{}", this.getId(), message, e);
                } else {
                    log.error("Error message from Dataflow[ID={}]:{}", this.getId(), message);
                }
            } else {
                if (e != null) {
                    log.error("Error message from Node[Name={}]: {}", node.getName(), message, e);
                } else {
                    log.error("Error message from Node[Name={}]: {}", node.getName(), message);
                }
            }
        }

/*
        for (Consumer<EtlLog> logListener : this.logListeners) {
            logListenerExecutor.execute(() -> {
                try {
                    logListener.accept(etlLog);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
*/
    }

    protected void writeLogOfNode(Node node, LogLevel logLevel, String message) {
        this.writeLogOfNode(node, logLevel, message, null);
    }

    private Row tryRead(InputNode inputNode, int _maxRetryCount) throws InterruptedException {
//        log.info("try read... [Node={}]", inputNode.asNode().getName());
        for (int retriedCount = 0; retriedCount < _maxRetryCount && this.isRunning(); retriedCount++) {
            try {
                Row row = inputNode.read();
                return row;
            } catch (NodeReadingException e) {
                String msg = String.format("InputNode reading exception！error message: %s\nReading will retry after %d seconds...", e.getMessage(), retryTimeoutSeconds);
                this.writeLogOfNode(inputNode.asNode(), LogLevel.ERROR, msg, e);
                Thread.sleep(retryTimeoutSeconds * 1000L);
            }
        }
        if (this.isRunning()) {
            this.writeLogOfNode(inputNode.asNode(), LogLevel.ERROR, "InputNode reading exceed max retry count " + _maxRetryCount);
        }
        return null;
    }

    private void readingAndWriteToPipes(InputNode inputNode) {
        String logMsg = String.format("%s start reading...", inputNode.asNode().getName());
        this.writeInfoLog(logMsg);

        long currentStartTime = System.currentTimeMillis();
        int readTotal = 0;
        try {
            while (this.isRunning()) {
                Row row = tryRead(inputNode, maxRetryCount);
                if (row == null) {
                    this.writeLogOfNode(inputNode.asNode(), LogLevel.ERROR, "InputNode reading failed, The dataflow will be terminated.");
                    this.tryStop(true);
                    break;
                }

                if (!row.isEnd()) {
                    readTotal++;
                    if (readTotal % processingThresholdLog == 0) {
                        double elapsedSeconds = (System.currentTimeMillis() - currentStartTime) / 1000.0;
                        elapsedSeconds = (elapsedSeconds == 0 ? 0.001 : elapsedSeconds);
                        double avgSpeed = (double) readTotal / (System.currentTimeMillis() - startTime) * 1000.0;
                        String msg = String.format("Total read = %d, current speed = %.0f records/s, average speed = %.0f records/s", readTotal, processingThresholdLog / elapsedSeconds, avgSpeed);
                        this.writeLogOfNode(inputNode.asNode(), LogLevel.INFO, msg);
                        currentStartTime = System.currentTimeMillis();
                    }
                } else {
                    String msg = String.format("InputNode reading completed，%d total.", readTotal);
                    this.writeLogOfNode(inputNode.asNode(), LogLevel.INFO, msg);
                }

                if (inputNode instanceof MiddleNode && ((MiddleNode) inputNode).getType() == MiddleNode.Type.SWITCH) {
                    for (int i = 0; i < inputNode.asNode().getNextPipes().size(); i++) {
                        Pipe nextPipe = inputNode.asNode().getNextPipes().get(i);
                        if (!nextPipe.isValid()) {
                            continue;
                        }

                        if (row.isEnd()) {
                            nextPipe.beWritten(row);
                            continue;
                        }

                        if (row.getPipeIndex() == i) {
                            nextPipe.beWritten(row);
                        }
                    }
                } else {
                    CountDownLatch countDownLatch = new CountDownLatch(inputNode.asNode().getNextPipes().size());//确保每个下游管道都接收到数据
                    for (Pipe nextPipe : inputNode.asNode().getNextPipes()) {
                        if (!nextPipe.isValid()) {
                            countDownLatch.countDown();
                            continue;
                        }

                        dataTransferThreadPool.execute(() -> {
                            try {
                                nextPipe.beWritten(row);
                            } catch (InterruptedException e) {
                                log.error("Pipe write exception!", e);
                                throw new RuntimeException(e);
                            } finally {
                                countDownLatch.countDown();
                            }
                        });
                    }
                    countDownLatch.await();//确保每个下游管道都接收到数据
                }

                if (row.isEnd()) {// reading completed , break the foreach
                    break;
                }
            }
        } catch (InterruptedException e) {
//            this.writeErrorLog(String.format("节点【%s】读取线程中断，异常消息：%s", inputNode.asNode().getId(), e.getMessage()), e);
            this.writeErrorLog(String.format("The Node[ID=%s] reading interrupted, Error message: %s", inputNode.asNode().getId(), e.getMessage()), e);
        }
    }

    private boolean tryWrite(Row row, OutputNode outputNode, int _maxRetryCount) throws InterruptedException {
        for (int retriedCount = 0; retriedCount < _maxRetryCount && this.isRunning(); retriedCount++) {
            try {
                outputNode.write(row);
                return true;
            } catch (NodeWritingException e) {
                this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, String.format("OutputNode reading exception！error message: %s\nWriting will retry after %d seconds...", e.getMessage(), retryTimeoutSeconds), e);
                Thread.sleep(retryTimeoutSeconds * 1000L);
            }
        }
        if (this.isRunning()) {
            this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, "OutputNode writing exceed max retry count " + _maxRetryCount);
        }
        return false;
    }

    private void readFromPipeAndWriting(OutputNode outputNode) {
        Pipe prevPipe = outputNode.asNode().getPrevPipe().orElseThrow(() -> new RuntimeException("Data flow configuration error: The previous pipeline is empty!"));
        allCompetedOutputCount.addAndGet(1);
        String logMsg = String.format("%s start writing...", outputNode.asNode().getName());
        this.writeInfoLog(logMsg);
        long currentNodeStartTime = System.currentTimeMillis();
        int writtenTotal = 0;
        try {
            while (this.isRunning()) {
                Row row = prevPipe.beRead();
                boolean success = this.tryWrite(row, outputNode, maxRetryCount);
                if (!success) {
                    this.tryStop(true);
                    break;
                }

                if (row == null || row.isEnd()) {
                    logMsg = String.format("Write completed, total: %d", writtenTotal);
                    this.writeLogOfNode(outputNode.asNode(), LogLevel.INFO, logMsg);
                    break;
                }

                if (++writtenTotal % processingThresholdLog == 0) {
                    double elapsed = System.currentTimeMillis() - currentNodeStartTime;
                    elapsed = (elapsed == 0 ? 0.001 : elapsed);
                    logMsg = String.format("Total output = %d, current speed = %.0f records/s, average speed = %.0f records/s, current pipeline (%d/%d)"
                            , writtenTotal
                            , processingThresholdLog / elapsed * 1000
                            , writtenTotal / ((System.currentTimeMillis() - startTime) / 1000.0)
                            , prevPipe.getCurrentBufferSize()
                            , prevPipe.getBufferCapacity()
                    );
                    this.writeLogOfNode(outputNode.asNode(), LogLevel.INFO, logMsg);
                    currentNodeStartTime = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, "Node writer thread interrupted. Exception message:" + e.getMessage(), e);
        }

        logMsg = String.format("Output finished. Total output = %d", writtenTotal);
        this.writeLogOfNode(outputNode.asNode(), LogLevel.INFO, logMsg);
        if (allCompetedOutputCount.addAndGet(-1) == 0) {
            this.writeInfoLog("All output nodes have finished writing. Preparing to shutdown the dataflow thread pool...");
            this.tryStop(false);
        }
    }

    private void recursiveStartNodes(Node currentNode) {
        this.writeLogOfNode(currentNode, LogLevel.INFO, "Start initializing...");
        try {
            currentNode.prestart(this);
        } catch (NodePrestartException e) {
            this.writeLogOfNode(currentNode,  LogLevel.ERROR, e.getMessage(), e);
            this.tryStop(true);
            return;
        }
        this.writeLogOfNode(currentNode, LogLevel.INFO, "Completed initialization.");

        for (Pipe nextPipe : currentNode.getNextPipes()) {
            if (nextPipe.isValid() && nextPipe.to().isPresent()) {
                nextPipe.initialize(this);
                this.recursiveStartNodes(nextPipe.to().get().asNode());
            }
        }

        if (currentNode instanceof InputNode) {
            this.dataTransferThreadPool.execute(() -> {
                try {
                    readingAndWriteToPipes((InputNode) currentNode);
                } catch (Exception e) {
                    this.writeLogOfNode(currentNode, LogLevel.ERROR, "Caught an unexpected exception! The dataflow will be terminated. Error message: " + e.getMessage(), e);
                    this.tryStop(true);
                }
            });
        }

        if (currentNode instanceof OutputNode) {
            this.dataTransferThreadPool.execute(() -> {
                try {
                    readFromPipeAndWriting((OutputNode) currentNode);
                } catch (Exception e) {
                    this.writeLogOfNode(currentNode, LogLevel.ERROR, "Caught an unexpected exception! The dataflow will be terminated. Error message: " + e.getMessage(), e);
                    this.tryStop(true);
                }
            });
        }

    }

    public void syncStart() throws DataflowException {
        syncStart(999, TimeUnit.DAYS);
    }


    public boolean syncStart(long timeout, TimeUnit timeUnit) {
        if (status == Status.STOPPED || status == Status.STOPPING) {
            throw new DataflowPrestartException("Startup failed. Dataflow has already ended!");
        }

        Date now = new Date();
        this.writeInfoLog("Starting dataflow[ID=" + this.id + "]...");
        this.startTime = now.getTime();
        this.status = Status.RUNNING;
        try {
            this.recursiveStartNodes(head);
        } catch (Exception e) {
            this.tryStop(true);
            this.writeErrorLog(e.getMessage(), e);
        }
        boolean notTimeout = false;
        try {
            notTimeout = this.dataTransferThreadPool.awaitTermination(timeout, timeUnit);
            endTime = System.currentTimeMillis();
            if (notTimeout) {
                this.writeInfoLog(String.format("Execution finished. Total time elapsed: %.2fs", (endTime - startTime) / 1000f));
            } else {
                this.dataTransferThreadPool.shutdownNow();
                this.writeErrorLog("Execution timeout! Forced shutdown has been executed!");
            }
        } catch (InterruptedException e) {
            endTime = System.currentTimeMillis();
            this.dataTransferThreadPool.shutdownNow();
            this.writeErrorLog(String.format("Task interrupted unexpectedly! Forced shutdown has been executed! Total time elapsed: %.2fs", (endTime - startTime) / 1000f), e);
        }

        if (this.dataTransferThreadPool.isTerminated()) {
            this.writeInfoLog("Dataflow thread pool shutdown successfully.");
        } else {
            this.writeErrorLog("Data flow thread pool failed to shut down normally!");
        }
        this.status = Status.STOPPED;
        return notTimeout;
    }

    private void tryStop(boolean force) throws DataflowStoppingException {
//        if (status == Status.STOPPING || status == Status.STOPPED) {
//            throw new DataflowStoppingException("Dataflow has been stopped!");
//        }

        if (status == Status.IDLE || status == Status.STOPPING || status == Status.STOPPED) {
            status = Status.STOPPED;
            return;
        }

        this.status = Status.STOPPING;
        DataflowHelper.of(this).forEachNodesAndPipes((node, pipe) -> {
            if (node != null) {
                node.onDataflowStop();
            } else if (pipe != null) {
                pipe.stop();
            }
            return true;
        });
        if (force) {
            this.dataTransferThreadPool.shutdownNow();
        } else {
            this.dataTransferThreadPool.shutdown();
        }
/*
        if (this.dataTransferThreadPool.isShutdown()) {
            this.writeInfoLog("Dataflow Thread pool are shutdown.");
        } else {
            this.writeErrorLog("Dataflow Thread pool are not shutdown yet!");
        }

        if (this.dataTransferThreadPool.isTerminated()) {
            this.writeInfoLog("Dataflow Thread pool has been terminated successfully.");
        } else {
            this.writeErrorLog("Dataflow Thread pool has not terminated yet!");
        }*/
    }

    public void asyncStop(long timeoutMillis)  throws DataflowStoppingException {
        this.tryStop(false);
        new Thread(() -> {
            try {
                Thread.sleep(timeoutMillis);
                if (!dataTransferThreadPool.isTerminated()) {
                    dataTransferThreadPool.shutdownNow();
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                status = Status.STOPPED;
            }
        }).start();
    }

    public void syncStop(long timeout, TimeUnit timeUnit) throws DataflowStoppingException {
        this.tryStop(false);
        String logMsg;
        try {
            boolean notTimeout = this.dataTransferThreadPool.awaitTermination(timeout, timeUnit);
            if (notTimeout) {
                logMsg = "Stop successful.";
                this.writeInfoLog(logMsg);
            } else {
                this.dataTransferThreadPool.shutdownNow();
                logMsg = "Stop timeout, Has executed forced termination.";
                this.writeErrorLog(logMsg);
                throw new DataflowStoppingException(logMsg);
            }
        } catch (InterruptedException e) {
            this.dataTransferThreadPool.shutdownNow();
            logMsg = "Stop interrupted! Forced shutdown has been executed!";
            this.writeErrorLog(logMsg, e);
            throw new DataflowStoppingException(logMsg);
        }
    }

    public void syncStopWithError(long timeout, TimeUnit timeUnit, String errorMsg) throws DataflowStoppingException {
        this.writeErrorLog(errorMsg);
        this.syncStop(timeout, timeUnit);
    }

    public void syncStopWithInfo(long timeout, TimeUnit timeUnit, String infoMsg) throws DataflowStoppingException {
        this.writeErrorLog(infoMsg);
        this.syncStop(timeout, timeUnit);
    }


//    public void awaitStoppingSignal() throws InterruptedException {
//        synchronized (blockLocker) {
//            blockLocker.wait();
//        }
//    }

    public boolean isRunning() {
        return status == Status.RUNNING;
    }

    public boolean isStopped() {
        return status == Status.STOPPED;
    }

//    protected ExecutorService getDataTransferExecutor() {
//        return dataTransferExecutor;
//    }

}
