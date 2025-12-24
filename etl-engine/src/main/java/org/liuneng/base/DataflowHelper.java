package org.liuneng.base;

import lombok.extern.slf4j.Slf4j;
import org.liuneng.util.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class DataflowHelper {

    private final Dataflow DATAFLOW;

    private DataflowHelper(Dataflow dataflow) {
        this.DATAFLOW = dataflow;
    }

    public static DataflowHelper of(Dataflow dataflow) {
        return new DataflowHelper(dataflow);
    }

    public void logListener(Consumer<EtlLog> handler) {

        Runnable runnable = () -> {
            int cursor = 0;
            while (!DATAFLOW.isStopped() || cursor < DATAFLOW.getLogList().size()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (cursor < DATAFLOW.getLogList().size()) {
                    EtlLog etlLog = DATAFLOW.getLogList().get(cursor);
                    if (etlLog == null) {
                        log.error("日志为空");
                    }
                    handler.accept(etlLog);
                    cursor++;
                }
            }

        };
        Thread thread = new Thread(runnable, "LogListener-" + DATAFLOW.getId());
        thread.start();
    }

    public List<Tuple2<Node, Pipe>> getAllNodesAndPipes() {
        List<Tuple2<Node, Pipe>> results = new ArrayList<>();
        recurNodes(DATAFLOW.getHead(), results::add);
        return results;
    }

    public Node findNodeById(String id) {
        AtomicReference<Node> re = new AtomicReference<>();
        this.forEachNodesOrPipes((node, pipe) -> {
            if (node != null && id.equals(node.getId())) {
                re.set(node);
                return false;
            }
            return true;
        });

        return re.get();
    }

    public void forEachNodesOrPipes(BiFunction<Node, Pipe, Boolean> consumer) {
        recurNodes(DATAFLOW.getHead(), nodePipeTuple2 -> consumer.apply(nodePipeTuple2.getPartA(), nodePipeTuple2.getPartB()));
    }

    private void recurNodes(Object currentNode, Function<Tuple2<Node, Pipe>, Boolean> handler) {
        if (currentNode instanceof Node) {
            Node node = (Node) currentNode;
            boolean isContinue = handler.apply(new Tuple2<>(node, null));
            if (!isContinue) {
                return;
            }
            if (!node.getNextPipes().isEmpty()) {
                for (Pipe nextPipe : node.getNextPipes()) {
                    recurNodes(nextPipe, handler);
                }
            }
        } else if (currentNode instanceof Pipe) {
            Pipe pipe = (Pipe) currentNode;
            boolean isContinue = handler.apply(new Tuple2<>(null, pipe));
            if (!isContinue) {
                return;
            }
            if (pipe.to().isPresent()) {
                recurNodes(pipe.to().get(), handler);
            }
        }
    }

}
