package org.liuneng.base;

import cn.hutool.core.util.IdUtil;
import lombok.Getter;
import lombok.Setter;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.util.StrUtil;

import java.util.*;
import java.util.stream.Collectors;

public abstract class Node {

    @Setter
    private String id;

    @Setter
    private String name;

    protected Dataflow dataflowInstance;

    private Pipe previousPipe;

    @Getter
    private final List<Pipe> nextPipes = new ArrayList<>();


    public Node() {
        this.id = "Node-"+IdUtil.fastSimpleUUID();
        this.name = this.getClass().getSimpleName();
    }


    public Optional<Pipe> getPreviousPipe() {
        return Optional.ofNullable(previousPipe);
    }

    public Optional<InputNode> getPreviousNode() {
        if (this.getPreviousPipe().isPresent()) {
            return this.getPreviousPipe().get().from();
        } else {
            return Optional.empty();
        }
    }

    public List<OutputNode> getNextNodes() {
        if (this.getNextPipes().isEmpty()) {
            return Collections.emptyList();
        } else {
            return this.getNextPipes().stream().map(pipe -> pipe.to().orElse(null)).collect(Collectors.toList());
        }
    }

    protected void setPreviousPipe(Pipe previousPipe) {
        this.previousPipe = previousPipe;
    }

    protected void setNextPipes(List<Pipe> nextPipes) {
        this.nextPipes.clear();
        this.nextPipes.addAll(nextPipes);
    }

    protected void addPreviousPipe(Pipe pipe) {
        nextPipes.add(pipe);
    }

    public String getId() {
        return StrUtil.isBlank(id) ? "" : id;
    }

    public String getName() {
        return StrUtil.isBlank(name) ? this.getClass().getSimpleName() : name;
    }

    protected Dataflow getDataflowInstance() {
        return dataflowInstance;
    }

    protected void setDataflowInstance(Dataflow dataflowInstance) {
        this.dataflowInstance = dataflowInstance;
    }

    protected void prestart(Dataflow dataflow) throws NodePrestartException {
        this.dataflowInstance = dataflow;

//        dataflow.getDataTransferExecutor().execute(() -> {
//            try {
//                this.dataflowInstance.awaitStoppingSignal();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            } finally {
//                this.onStop();
//            }
//        });

    };

    protected abstract void onDataflowStop();

    protected void writeInfoLog(String message) {
        this.dataflowInstance.writeLogOfNode(this, LogLevel.INFO, message, null);
    }

    protected void writeErrorLog(String message) {
        this.dataflowInstance.writeLogOfNode(this, LogLevel.ERROR, message, null);
    }
}