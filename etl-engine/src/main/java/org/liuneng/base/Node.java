package org.liuneng.base;

import lombok.Getter;
import lombok.Setter;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.util.StrUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class Node {

    @Setter
    @Getter
    private String id;

    @Setter
    private String name;

    protected Dataflow dataflowInstance;

    private Pipe beforePipe;

    @Getter
    private final List<Pipe> afterPipes = new ArrayList<>();

    public Optional<Pipe> getBeforePipe() {
        return Optional.ofNullable(beforePipe);
    }

    public Optional<InputNode> getBeforeNode() {
        if (this.getBeforePipe().isPresent()) {
            return this.getBeforePipe().get().getFrom();
        } else {
            return Optional.empty();
        }
    }

    public List<OutputNode> getAfterNodes() {
        if (this.getAfterPipes().isEmpty()) {
            return Collections.emptyList();
        } else {
            return this.getAfterPipes().stream().map(pipe -> pipe.getTo().orElse(null)).collect(Collectors.toList());
        }
    }

    protected void setBeforePipe(Pipe beforePipe) {
        this.beforePipe = beforePipe;
    }

    protected void setAfterPipes(List<Pipe> afterPipes) {
        this.afterPipes.clear();
        this.afterPipes.addAll(afterPipes);
    }

    protected void addAfterPipe(Pipe pipe) {
        afterPipes.add(pipe);
    }

    public String getName() {
        return StrUtil.isBlank(name) ? id : name;
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

}