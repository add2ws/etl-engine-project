package org.liuneng.base;

import cn.hutool.core.util.IdUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.exception.*;
import org.liuneng.util.DataflowHelper;
import org.liuneng.util.StrUtil;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    private final LocalDateTime createTime;

    private int increasedId = 0;

    private final ExecutorService dataTransferExecutor;

//    protected final Object blockLocker  = new Object();;

    @Getter
    private long startTime;

    @Getter
    private long endTime;

    @Getter
    private final List<EtlLog> logList = new ArrayList<>();

    @Getter
    @Setter
    private boolean printLogToConsole = true;

    /*private final List<Consumer<EtlLog>> logListeners = new ArrayList<>();

    public void addLogListener(Consumer<EtlLog> logListener) {
        logListeners.add(logListener);
    }

    public void removeLogListener(Consumer<EtlLog> logListener) {
        logListeners.remove(logListener);
    }*/

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

        if  (logLevel == LogLevel.INFO) {
            if (node == null) {
                log.info("数据流[ID={}]日志消息：{}", this.getId(), message);
            } else {
                log.info("节点：{} 的日志消息：{}", node.getName(), message);
            }
        } else if (logLevel == LogLevel.ERROR) {
            if (node == null) {
                log.error("数据流[ID={}]异常消息：{}", this.getId(), message, e);
            } else {
                log.error("节点：{} 的异常消息：{}", node.getName(), message, e);
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

    private void readingFrom(InputNode inputNode) {
        dataTransferExecutor.execute(() -> {
            String logMsg = String.format("%s start reading...", inputNode.asNode().getName());
            this.writeInfoLog(logMsg);

            long currentStartTime = System.currentTimeMillis();
            int readTotal = 0;

            int errorCount = 0;
            while (this.isRunning()) {
                try {
                    Row row;
                    try {
                        row = inputNode.read();
                    } catch (NodeReadingException e) {
                        if (++errorCount <= maxRetryCount) {
                            String msg = String.format("InputNode reading exception！%s will retry after 5 seconds...", e.getMessage());
                            this.writeLogOfNode(inputNode.asNode(), LogLevel.ERROR, msg);
                            Thread.sleep(3000);
                            continue;
                        } else {
                            this.writeLogOfNode(inputNode.asNode(), LogLevel.ERROR, "已重试" + maxRetryCount + "次仍异常，数据流强制结束。");
                            break;
                        }

                    }
                    errorCount = 0;

                    if (row != null && !row.isEnd()) {
                        readTotal++;
                    }

                    if (readTotal % processingThresholdLog == 0) {
                        double elapsedSeconds = (System.currentTimeMillis() - currentStartTime) / 1000.0;
                        elapsedSeconds = (elapsedSeconds == 0 ? 0.001 : elapsedSeconds);
//                        double currentSpeed = ;
                        double avgSpeed = (double) readTotal / (System.currentTimeMillis() - startTime) * 1000.0;
                        String msg = String.format("读取总量=%d, 当前速度=%.0f条/秒，平均速度%.0f条/秒", readTotal, processingThresholdLog / elapsedSeconds, avgSpeed);
                        this.writeLogOfNode(inputNode.asNode(), LogLevel.INFO, msg);
                        currentStartTime = System.currentTimeMillis();
                    }

                    CountDownLatch countDownLatch = new CountDownLatch(inputNode.asNode().getNextPipes().size());//确保每个下游管道都接收到数据

                    if (inputNode instanceof MiddleNodeSwitch) {
                        for (int i = 0; i < inputNode.asNode().getNextPipes().size(); i++) {

                        }

                    } else {

                        for (Pipe nextPipe : inputNode.asNode().getNextPipes()) {
                            if (!nextPipe.isValid()) {
                                countDownLatch.countDown();
                                continue;
                            }

                            dataTransferExecutor.execute(() -> {
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
                    }
                    countDownLatch.await();//确保每个下游管道都接收到数据

                    if (row == null || row.isEnd()) {
                        String msg = String.format("InputNode reading completed，%d total.", readTotal);
                        this.writeLogOfNode(inputNode.asNode(), LogLevel.INFO, msg);
                        break;//节点数据完全读取结束
                    }

                } catch (InterruptedException e) {
                    this.writeErrorLog(String.format("节点【%s】读取线程中断，异常消息：%s", inputNode.asNode().getId(), e.getMessage()), e);
                }
            }
        });
    }

    private void processingOf(MiddleNode_new middleNode) {

        Pipe previousPipe = middleNode.asNode().getPreviousPipe().orElseThrow(() -> new DataflowException("Previous pipe is null!"));

        try {
            Row row = previousPipe.beRead();




        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void writingTo(OutputNode outputNode) {
        Pipe previousPipe = outputNode.asNode().getPreviousPipe().orElseThrow(() -> new RuntimeException("数据流配置有误，获取前一个管道为空！"));
        allCompetedOutputCount.addAndGet(1);
        dataTransferExecutor.execute(() -> {
            String logMsg = String.format("%s start writing...", outputNode.asNode().getName());
            this.writeInfoLog(logMsg);

            long currentNodeStartTime = System.currentTimeMillis();
            int writtenTotal = 0;
            int errorCount = 0;
            while (this.isRunning()) {
                try {
                    Row row = null;
                    if (errorCount == 0) {
                        row = previousPipe.beRead();
                    }
                    try {
                        outputNode.write(row);
                    } catch (NodeWritingException e) {
                        if (++errorCount <= maxRetryCount) {
                            logMsg = String.format("写入异常！%s 3秒后重试。。。", e.getMessage());
                            this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, logMsg);
                            Thread.sleep(3000);
                            continue;
                        } else {
                            this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, "已重试" + maxRetryCount + "次仍异常，数据流强制结束。");
                            break;
                        }
                    }
                    errorCount = 0;

                    if (row == null || row.isEnd()) {
                        logMsg = String.format("写入结束，共%d条。", writtenTotal);
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
                                , previousPipe.getCurrentBufferSize()
                                , previousPipe.getBufferCapacity()
                        );
                        this.writeLogOfNode(outputNode.asNode(), LogLevel.INFO, logMsg);
                        currentNodeStartTime = System.currentTimeMillis();
                    }
                } catch (InterruptedException e) {
                    this.writeLogOfNode(outputNode.asNode(), LogLevel.ERROR, "节点写入线程中断，异常消息：" + e.getMessage());
                }
            }

            logMsg = String.format("输出结束，输出总量=%d", writtenTotal);
            this.writeLogOfNode(outputNode.asNode(), LogLevel.INFO, logMsg);
            if (allCompetedOutputCount.addAndGet(-1) == 0) {
                this.writeInfoLog("所有输出节点已写入结束，准备结束数据流线程池。。。");
                this.tryStop();
            }
        });
    }

    private void recursiveStartNodes(Node currentNode) {
        String timeStr = createTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        if (StrUtil.isBlank(currentNode.getId())) {
            String nodeID = String.format("%s-%d_%s", currentNode.getClass().getSimpleName(), ++increasedId, timeStr);
            currentNode.setId(nodeID);
        }
        this.writeLogOfNode(currentNode, LogLevel.INFO, "开始初始化...");
        currentNode.prestart(this);
//        try {
//        } catch (NodePrestartException e) {
//            throw new Exception(String.format("节点[%s]初始化失败！error message: %s", currentNode.getId(), e.getMessage()));
//        }
        this.writeLogOfNode(currentNode, LogLevel.INFO, "初始化完成。");

        if (currentNode instanceof InputNode) {
            readingFrom((InputNode) currentNode);
        }

        if (currentNode instanceof OutputNode) {
            writingTo((OutputNode) currentNode);
        }

        for (Pipe nextPipe : currentNode.getNextPipes()) {
            if (nextPipe.isValid() && nextPipe.to().isPresent()) {
                if (StrUtil.isBlank(nextPipe.getId())) {
                    String pipeID = String.format("Pipeline-%d_%s", ++increasedId, timeStr);
                    nextPipe.setId(pipeID);
                }
                nextPipe.initialize(this);
                this.recursiveStartNodes(nextPipe.to().get().asNode());
            }
        }
    }


    public Dataflow(Node headNode) {
        this.id = "Dataflow"+IdUtil.fastSimpleUUID();
        this.head = headNode;
        this.createTime = LocalDateTime.now();
        this.status = Status.IDLE;
        this.dataTransferExecutor = Executors.newCachedThreadPool(r -> new Thread(r, "DataTransmissionThread-" + this.id));
    }

    public void syncStart() throws DataflowException {
        syncStart(999, TimeUnit.DAYS);
    }


    public boolean syncStart(long timeout, TimeUnit timeUnit) {
        if (status == Status.STOPPED || status == Status.STOPPING) {
            throw new DataflowPrestartException("启动失败，数据流已经结束！");
        }

        String logMsg;
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
            notTimeout = this.dataTransferExecutor.awaitTermination(timeout, timeUnit);
            endTime = System.currentTimeMillis();
            if (notTimeout) {
                this.writeInfoLog(String.format("运行结束，总耗时%.2fs", (endTime - startTime) / 1000f));
            } else {
                this.dataTransferExecutor.shutdownNow();
                this.writeErrorLog("运行超时!已执行强制关闭！");
            }
        } catch (InterruptedException e) {
            endTime = System.currentTimeMillis();
            this.dataTransferExecutor.shutdownNow();
            this.writeErrorLog(String.format("任务意外中断，已执行强制关闭！总耗时%.2fs", (endTime - startTime) / 1000f), e);
        }

        if (this.dataTransferExecutor.isTerminated()) {
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
        DataflowHelper.forEachNodesOrPipes(this, (node, pipe) -> {
            this.dataTransferExecutor.execute(() -> {
                if (node != null) {
                    node.onDataflowStop();
                } else if (pipe != null) {
                    pipe.stop();
                }
            });
            return true;
        });
        this.dataTransferExecutor.shutdown();
    }

    public void asyncStop(long timeoutMillis) {
        this.tryStop();
        new Thread(() -> {
            try {
                Thread.sleep(timeoutMillis);
                if (!dataTransferExecutor.isTerminated()) {
                    dataTransferExecutor.shutdownNow();
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
            boolean notTimeout = this.dataTransferExecutor.awaitTermination(timeout, timeUnit);
            if (notTimeout) {
                logMsg = "停止成功。";
                this.writeInfoLog(logMsg);
            } else {
                this.dataTransferExecutor.shutdownNow();
                logMsg = "停止超时，已执行强制关闭！";
                this.writeErrorLog(logMsg);
                throw new DataflowStoppingException(logMsg);
            }
        } catch (InterruptedException e) {
            this.dataTransferExecutor.shutdownNow();
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
