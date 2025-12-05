package org.liuneng.base;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.IdUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.exception.*;
import org.liuneng.util.DataflowHelper;
import org.liuneng.util.StrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /*private final List<Consumer<EtlLog>> logListeners = new ArrayList<>();

    public void addLogListener(Consumer<EtlLog> logListener) {
        logListeners.add(logListener);
    }

    public void removeLogListener(Consumer<EtlLog> logListener) {
        logListeners.remove(logListener);
    }*/

    public void addInfoLog(String message) {
        addLogByNodeID(null, LogLevel.INFO, message);
    }

    public void addErrorLog(String message) {
        addLogByNodeID(null, LogLevel.ERROR, message);
    }

    private void addLogByNodeID(String nodeID, LogLevel logLevel, String message) {
        EtlLog etlLog = new EtlLog();
        etlLog.setId(IdUtil.fastSimpleUUID());
        etlLog.setNodeID(nodeID);
        etlLog.setLogLevel(logLevel);
        etlLog.setMessage(message);
        etlLog.setTimestamp(System.currentTimeMillis());
        logList.add(etlLog);
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


    private void nodeReading(InputNode inputNode) {
        dataTransferExecutor.execute(() -> {
            String s1 = String.format("%s start reading...", inputNode.asNode().getName());
            log.info(s1);
            addInfoLog(s1);

            long time = System.currentTimeMillis();
            int readTotal = 0;

            try {

                Row row = null;
                int errorCount = 0;
                while (this.isRunning()) {
                    try {
                        if (errorCount == 0) {
                            row = inputNode.read();
                        }
                        errorCount = 0;

                    } catch (Exception e) {
                        errorCount++;
                        if (errorCount > maxRetryCount) {
                            log.error("节点【{}】读取时已重试{}次仍异常，数据流强制结束。", inputNode.asNode().getId(), maxRetryCount);
                            this.addLogByNodeID(inputNode.asNode().getId(), LogLevel.ERROR, "已重试"+maxRetryCount+"次仍异常，数据流强制结束。");
                            break;
                        } else {
                            String msg = String.format("InputNode reading exception！%s will retry after 5 seconds...", e.getMessage());
                            log.error(msg);
                            log.debug(msg, e);
                            this.addLogByNodeID(inputNode.asNode().getId(), LogLevel.ERROR, msg);
                            Thread.sleep(5000);
                            continue;
                        }

                    }

                    CountDownLatch countDownLatch = new CountDownLatch(inputNode.asNode().getAfterPipes().size()); //确保每个下游节点都写完数据，再进入下一轮读取
                    for (Pipe afterPipe : inputNode.asNode().getAfterPipes()) {
                        if (!afterPipe.isValid()) {
                            countDownLatch.countDown();
                            continue;
                        }

                        Row finalRow = row;
                        dataTransferExecutor.execute(() -> {
                            try {
                                log.trace("开始写入pipe。。。{}", finalRow);
                                afterPipe.write(finalRow);
                            } catch (InterruptedException e) {
                                log.error("Pipe 写入异常!");
                                throw new RuntimeException(e);
                            }
                            countDownLatch.countDown();
                        });
                    }
                    countDownLatch.await();

                    if (row == null || row.isEnd()) {
                        String msg = String.format("InputNode reading completed，%d total.", readTotal);
                        log.info(msg);
                        this.addLogByNodeID(inputNode.asNode().getId(), LogLevel.INFO, msg);
                        break;
                    }

                    readTotal++;
                    if (readTotal % processingThresholdLog == 0) {
                        double elapsedSeconds = (System.currentTimeMillis() - time) / 1000.0;
                        elapsedSeconds = (elapsedSeconds == 0 ? 0.001 : elapsedSeconds);
                        String msg = String.format("输入节点[%s] 读取总量=%d, 当前速度=%.0f条/秒，平均速度%.0f条/秒", inputNode.asNode().getName(), readTotal, processingThresholdLog / elapsedSeconds, readTotal / ((System.currentTimeMillis() - startTime) / 1000.0));
                        log.info(msg);
                        this.addLogByNodeID(inputNode.asNode().getId(), LogLevel.INFO, msg);
                        time = System.currentTimeMillis();
                    }
                }
            } catch (InterruptedException e) {
                log.error("节点【{}】读取线程中断，异常消息：{}", inputNode.asNode().getId(), e.getMessage(), e);
            }
        });
    }

    private void nodeWriting(OutputNode outputNode) {
        Pipe beforePipe = outputNode.asNode().getBeforePipe().orElseThrow(() -> new RuntimeException("数据流配置有误，获取前一个管道为空！"));
        allCompetedOutputCount.addAndGet(1);
        dataTransferExecutor.execute(() -> {
            String logMessage = String.format("%s start writing...", outputNode.asNode().getName());
            log.info(logMessage);
            addInfoLog(logMessage);

            int writtenTotal = 0;
            int errorCount = 0;
            Row row = null;

            long time = System.currentTimeMillis();
            //region writing loop
            try {
                while (this.isRunning()) {
                    if (errorCount == 0) {
                        row = beforePipe.read();
                    }

                    log.trace("开始输出");
                    try {
                        outputNode.write(row);
                        errorCount = 0;

                        if (row.isEnd()) {
                            logMessage = String.format("节点[%s] 写入结束，共%d条。", outputNode.asNode().getName(), writtenTotal);
                            log.info(logMessage);
                            this.addLogByNodeID(outputNode.asNode().getId(), LogLevel.INFO, logMessage);
                            break;
                        }
                    } catch (NodeWritingException e) {
                        errorCount++;
                        if (errorCount <= maxRetryCount) {
                            logMessage = String.format("输出节点[%s] 写入异常！%s 3秒后重试。。。", outputNode.asNode().getId(), e.getMessage());
                            log.error(logMessage, e);
                            log.debug(logMessage, e);
                            this.addLogByNodeID(outputNode.asNode().getId(), LogLevel.ERROR, logMessage);
                            Thread.sleep(3000);
                            continue;
                        } else {
                            log.error("节点【{}】写入时已重试{}次仍异常，数据流强制结束。", outputNode.asNode().getId(), maxRetryCount);
                            this.addLogByNodeID(outputNode.asNode().getId(), LogLevel.ERROR, "已重试"+maxRetryCount+"次仍异常，数据流强制结束。");
                            break;
                        }
                    }
                    writtenTotal++;

                    if (writtenTotal % processingThresholdLog == 0) {
                        double elapsed = System.currentTimeMillis() - time;
                        elapsed = (elapsed == 0 ? 0.001 : elapsed);
                        logMessage = String.format("输出节点[%s] 输出总量=%d, 当前速度=%.0f条/秒，平均速度%.0f条/秒，当前管道(%d/%d)", outputNode.asNode().getName(), writtenTotal, processingThresholdLog / elapsed * 1000, writtenTotal / ((System.currentTimeMillis() - startTime) / 1000.0), beforePipe.getCurrentBufferSize(), beforePipe.getBufferCapacity());
                        log.info(logMessage);
                        this.addLogByNodeID(outputNode.asNode().getId(), LogLevel.INFO, logMessage);
                        time = System.currentTimeMillis();
                    }
                }
            } catch (InterruptedException e) {
                log.error("节点【{}】写入线程中断，异常消息：{}", outputNode.asNode().getId(), e.getMessage(), e);
            }
            //endregion writing loop
            logMessage = String.format("节点[%s] 输出结束，输出总量=%d", outputNode.asNode().getId(), writtenTotal);
            log.info(logMessage);
            this.addLogByNodeID(outputNode.asNode().getId(), LogLevel.INFO, logMessage);
            if (allCompetedOutputCount.addAndGet(-1) == 0) {
                log.debug("所有输出节点已结束，准备结束数据流线程池。。。");
                this.addInfoLog("所有输出节点已结束，准备结束数据流线程池。。。");
                this.stopping();
            }
        });
    }

    private void recursiveStartNodes(Node currentNode) {
        String timeStr = createTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        if (StrUtil.isBlank(currentNode.getId())) {
            String nodeID = String.format("%s-%d_%s", currentNode.getClass().getSimpleName(), ++increasedId, timeStr);
            currentNode.setId(nodeID);
        }
        this.addLogByNodeID(currentNode.getId(), LogLevel.INFO, String.format("节点[%s] 开始初始化...", currentNode.getName()));
        currentNode.prestart(this);
//        try {
//        } catch (NodePrestartException e) {
//            throw new Exception(String.format("节点[%s]初始化失败！error message: %s", currentNode.getId(), e.getMessage()));
//        }
        this.addLogByNodeID(currentNode.getId(), LogLevel.INFO, String.format("节点[%s] 初始化完成。", currentNode.getName()));

        if (currentNode instanceof InputNode) {
            nodeReading((InputNode) currentNode);
        }

        if (currentNode instanceof OutputNode) {
            nodeWriting((OutputNode) currentNode);
        }

        for (Pipe afterPipe : currentNode.getAfterPipes()) {
            if (afterPipe.isValid() && afterPipe.getTo().isPresent()) {
                if (StrUtil.isBlank(afterPipe.getId())) {
                    String pipeID = String.format("Pipeline-%d_%s", ++increasedId, timeStr);
                    afterPipe.setId(pipeID);
                }
                afterPipe.initialize(this);
                this.recursiveStartNodes(afterPipe.getTo().get().asNode());
            }
        }
    }


    public Dataflow(Node headNode) {
        this.id = IdUtil.fastSimpleUUID();
        this.head = headNode;
        this.createTime = LocalDateTime.now();
        this.status = Status.IDLE;
        this.dataTransferExecutor = Executors.newCachedThreadPool(r -> new Thread(r, "DataTransmissionThread-" + this.id));
    }

    public void syncStart() throws DataflowException {
        syncStart(1, TimeUnit.DAYS);
    }


    public boolean syncStart(long timeout, TimeUnit timeUnit) {
        if (status == Status.STOPPED || status == Status.STOPPING) {
            throw new DataflowPrestartException("启动失败，数据流已经结束！");
        }


        Date now = new Date();
        this.addInfoLog(String.format("%s 数据流[%s]开始执行。。。", DateUtil.format(now, "yyyy-MM-dd HH:mm:ss"), this.getId()));
        this.startTime = now.getTime();
        this.status = Status.RUNNING;
        try {
            this.recursiveStartNodes(head);
        } catch (Exception e) {
            this.stopping();
            this.addErrorLog(e.getMessage());
        }
        boolean notTimeout = false;
        try {
            notTimeout = this.dataTransferExecutor.awaitTermination(timeout, timeUnit);
            endTime = System.currentTimeMillis();
            if (notTimeout) {
                this.addInfoLog(String.format("数据流[%s]运行结束，总耗时%.2fs", this.getId(), (endTime - startTime) / 1000f));
            } else {
                this.dataTransferExecutor.shutdownNow();
                this.addErrorLog(String.format("数据流[%s]运行超时!已执行强制关闭！", this.getId()));
            }
        } catch (InterruptedException e) {
            endTime = System.currentTimeMillis();
            this.dataTransferExecutor.shutdownNow();
            this.addErrorLog(String.format("数据流[%s]任务意外中断，已执行强制关闭！总耗时%.2fs", this.getId(), (endTime - startTime) / 1000f));
        }

        if (this.dataTransferExecutor.isTerminated()) {
            this.addInfoLog("数据流线程池关闭正常。");
            log.info("数据流[{}]线程池关闭正常。", this.getId());
        } else {
            this.addErrorLog("数据流线程池未正常关闭！");
            log.error("数据流[{}]线程池关闭异常！", this.getId());
        }
        this.status = Status.STOPPED;
        return notTimeout;
    }

    private void stopping() {
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
        this.stopping();
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
        this.stopping();
        try {
            boolean notTimeout = this.dataTransferExecutor.awaitTermination(timeout, timeUnit);
            if (notTimeout) {
                this.addInfoLog(String.format("数据流[%s]停止成功。", this.getId()));
            } else {
                this.dataTransferExecutor.shutdownNow();
                this.addErrorLog(String.format("数据流[%s]停止超时，已执行强制关闭！", this.getId()));
                throw new DataflowStoppingException(String.format("数据流[%s]停止超时，已执行强制关闭！", this.getId()));
            }
        } catch (InterruptedException e) {
            this.dataTransferExecutor.shutdownNow();
            this.addErrorLog(String.format("数据流[%s]停止被中断，已执行强制关闭！", this.getId()));
            throw new DataflowStoppingException(String.format("数据流[%s]停止被中断，已执行强制关闭！", this.getId()));
        }
    }

    public void syncStopWithError(long timeout, TimeUnit timeUnit, String errorMsg) throws DataflowStoppingException {
        this.addErrorLog(errorMsg);
        this.syncStop(timeout, timeUnit);
    }

    public void syncStopWithInfo(long timeout, TimeUnit timeUnit, String infoMsg) throws DataflowStoppingException {
        this.addErrorLog(infoMsg);
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
