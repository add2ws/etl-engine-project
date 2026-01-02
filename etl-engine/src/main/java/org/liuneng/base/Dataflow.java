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

    @Getter
    @Setter
    private int retryTimeoutSeconds = 3;

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
                log.error("Error message from Dataflow[ID={}]:{}", this.getId(), message, e);
            } else {
                log.error("Error message from Node[Name={}]: {}", node.getName(), message, e);
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

        for (int retriedCount = 0; retriedCount < _maxRetryCount; retriedCount++) {
            try {
                Row row = inputNode.read();
                return row;
            } catch (NodeReadingException e) {
                String msg = String.format("InputNode reading exception！%s will retry after %d seconds...", e.getMessage(), retryTimeoutSeconds);
                this.writeLogOfNode(inputNode.asNode(), LogLevel.ERROR, msg, e);
                Thread.sleep(retryTimeoutSeconds * 1000L);
            }
        }
        this.writeLogOfNode(inputNode.asNode(), LogLevel.ERROR, "Exceed max retry count " + _maxRetryCount);
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
                    this.tryStop();
                    break;
                }

                if (!row.isEnd()) {
                    readTotal++;
                    if (readTotal % processingThresholdLog == 0) {
                        double elapsedSeconds = (System.currentTimeMillis() - currentStartTime) / 1000.0;
                        elapsedSeconds = (elapsedSeconds == 0 ? 0.001 : elapsedSeconds);
                        double avgSpeed = (double) readTotal / (System.currentTimeMillis() - startTime) * 1000.0;
                        String msg = String.format("读取总量=%d, 当前速度=%.0f条/秒，平均速度%.0f条/秒", readTotal, processingThresholdLog / elapsedSeconds, avgSpeed);
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
                                log.error("Pipe 写入异常!", e);
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
            this.writeErrorLog(String.format("节点【%s】读取线程中断，异常消息：%s", inputNode.asNode().getId(), e.getMessage()), e);
        }
    }

    /*private Row tryProcess(Row row, MiddleNode node, int _maxRetryCount) throws InterruptedException {
        for (int i = 0; i < _maxRetryCount; i++) {
            try {
                Row processed = node.process(row);
                return processed;
            } catch (NodeException e) {
                String msg = String.format("MiddleNode processing failed %d times, Will retry after 5 seconds... errMsg: %s", i+1, e.getMessage());
                this.writeLogOfNode(node.asNode(), LogLevel.ERROR, msg, e);
                Thread.sleep(retryTimeoutSeconds * 1000L);
            }
        }
        this.writeLogOfNode(node.asNode(), LogLevel.ERROR, "Exceed max retry count " + _maxRetryCount);
        return null;
    }*/

    /*private void processingOf(MiddleNode middleNode) {
        Pipe prevPipe = middleNode.asNode().getPrevPipe().orElseThrow(() -> new DataflowException("Previous pipe is null!"));

        try {
            while (this.isRunning()) {
                Row processedRow = tryProcess(prevPipe.beRead(), middleNode, maxRetryCount);
                if  (processedRow == null) {
                    if (middleNode.getFailedPolicy() == MiddleNode.FailedPolicy.TERMINATE_DATAFLOW) {
                        this.writeLogOfNode(middleNode.asNode(), LogLevel.ERROR, "Data processing failed, The dataflow will be terminated.");
                        this.tryStop();
                        break;
                    } else if (middleNode.getFailedPolicy() == MiddleNode.FailedPolicy.END_DOWNSTREAM) {
                        this.writeLogOfNode(middleNode.asNode(), LogLevel.ERROR, "Data processing failed, All downstream nodes will be end.");
                        processedRow = Row.ofEnd();
                    }
                }

                if (middleNode.getType() == MiddleNode.Type.COPY) {
                    CountDownLatch countDownLatch = new CountDownLatch(middleNode.asNode().getNextPipes().size());//确保每个下游管道都接收到数据
                    for (Pipe nextPipe : middleNode.asNode().getNextPipes()) {
                        if (!nextPipe.isValid()) {
                            countDownLatch.countDown();
                            continue;
                        }

                        Row finalProcessedRow = processedRow;
                        dataTransferThreadPool.execute(() -> {
                            try {
                                nextPipe.beWritten(finalProcessedRow);
                            } catch (InterruptedException e) {
                                log.error("Pipe 写入异常!", e);
                                throw new RuntimeException(e);
                            } finally {
                                countDownLatch.countDown();
                            }
                        });
                    }
                    countDownLatch.await();//确保每个下游管道都接收到数据
                } else if (middleNode.getType() == MiddleNode.Type.SWITCH) {
                    for (int i = 0; i < middleNode.asNode().getNextPipes().size(); i++) {
                        Pipe nextPipe = middleNode.asNode().getNextPipes().get(i);
                        if (!nextPipe.isValid()) {
                            continue;
                        }

                        if (processedRow.getPipeIndex() == i) {
                            nextPipe.beWritten(processedRow);
                        }
                    }
                }

            }

        } catch (InterruptedException e) {
            this.writeErrorLog("MiddleNode processing thread was interrupted.", e);
        }

    }*/

    private boolean tryWrite(Row row, OutputNode outputNode, int _maxRetryCount) throws InterruptedException {
        for (int retriedCount = 0; retriedCount < _maxRetryCount; retriedCount++) {
            try {
                outputNode.write(row);
                return true;
            } catch (NodeWritingException e) {
                this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, String.format("写入异常！%s 3秒后重试。。。", e.getMessage()));
                Thread.sleep(retryTimeoutSeconds * 1000L);
            }
        }
        this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, String.format("已重试%d次仍异常，节点强制结束。", _maxRetryCount));
        return false;
    }

    private void readFromPipeAndWriting(OutputNode outputNode) {
        Pipe prevPipe = outputNode.asNode().getPrevPipe().orElseThrow(() -> new RuntimeException("数据流配置有误，获取前一个管道为空！"));
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
                    logMsg = String.format("输出总量=%d, 当前速度=%.0f条/秒，平均速度%.0f条/秒，当前管道(%d/%d)"
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
            this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, "节点写入线程中断，异常消息：" + e.getMessage());
        }

        logMsg = String.format("输出结束，输出总量=%d", writtenTotal);
        this.writeLogOfNode(outputNode.asNode(), LogLevel.INFO, logMsg);
        if (allCompetedOutputCount.addAndGet(-1) == 0) {
            this.writeInfoLog("所有输出节点已写入结束，准备结束数据流线程池。。。");
            this.tryStop();
        }
    }

    private void recursiveStartNodes(Node currentNode) {
        dataTransferThreadPool.execute(() -> {
            this.writeLogOfNode(currentNode, LogLevel.INFO, "开始初始化...");
            currentNode.prestart(this);
            this.writeLogOfNode(currentNode, LogLevel.INFO, "初始化完成。");

            if (currentNode instanceof InputNode) {
                readingAndWriteToPipes((InputNode) currentNode);
            }

            if (currentNode instanceof OutputNode) {
                readFromPipeAndWriting((OutputNode) currentNode);
            }

//            if (currentNode instanceof MiddleNode) {
//                processingOf((MiddleNode) currentNode);
//            }
        });


        for (Pipe nextPipe : currentNode.getNextPipes()) {
            if (nextPipe.isValid() && nextPipe.to().isPresent()) {
                nextPipe.initialize(this);
                this.recursiveStartNodes(nextPipe.to().get().asNode());
            }
        }
    }

    public void syncStart() throws DataflowException {
        syncStart(999, TimeUnit.DAYS);
    }


    public boolean syncStart(long timeout, TimeUnit timeUnit) {
        if (status == Status.STOPPED || status == Status.STOPPING) {
            throw new DataflowPrestartException("启动失败，数据流已经结束！");
        }

        Date now = new Date();
        this.writeInfoLog("开始执行。。。");
        this.startTime = now.getTime();
        this.status = Status.RUNNING;
        try {
            this.recursiveStartNodes(head);
        } catch (Exception e) {
            this.tryStop();
            this.writeErrorLog(e.getMessage(), e);
        }
        boolean notTimeout = false;
        try {
            notTimeout = this.dataTransferThreadPool.awaitTermination(timeout, timeUnit);
            endTime = System.currentTimeMillis();
            if (notTimeout) {
                this.writeInfoLog(String.format("运行结束，总耗时%.2fs", (endTime - startTime) / 1000f));
            } else {
                this.dataTransferThreadPool.shutdownNow();
                this.writeErrorLog("运行超时!已执行强制关闭！");
            }
        } catch (InterruptedException e) {
            endTime = System.currentTimeMillis();
            this.dataTransferThreadPool.shutdownNow();
            this.writeErrorLog(String.format("任务意外中断，已执行强制关闭！总耗时%.2fs", (endTime - startTime) / 1000f), e);
        }

        if (this.dataTransferThreadPool.isTerminated()) {
            this.writeInfoLog("数据流线程池关闭正常。");
        } else {
            this.writeErrorLog("数据流线程池未正常关闭！");
        }
        this.status = Status.STOPPED;
        return notTimeout;
    }

    private void tryStop() {
        if (status == Status.STOPPING || status == Status.STOPPED) {
            return;
        }

        if (status == Status.IDLE) {
            status = Status.STOPPED;
            return;
        }

        this.status = Status.STOPPING;
        DataflowHelper.of(this).forEachNodesAndPipes((node, pipe) -> {
            this.dataTransferThreadPool.execute(() -> {
                if (node != null) {
                    node.onDataflowStop();
                } else if (pipe != null) {
                    pipe.stop();
                }
            });
            return true;
        });
        this.dataTransferThreadPool.shutdown();
    }

    public void asyncStop(long timeoutMillis) {
        this.tryStop();
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
        this.tryStop();
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
            logMsg = "停止被中断，已执行强制关闭！";
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
