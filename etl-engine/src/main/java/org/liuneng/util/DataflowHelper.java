package org.liuneng.util;

import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.Dataflow;
import org.liuneng.base.EtlLog;
import org.liuneng.base.Node;
import org.liuneng.base.Pipe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class DataflowHelper {

    private DataflowHelper() {}

    public static void logListener(Dataflow dataflow, Consumer<EtlLog> handler) {
        Runnable runnable = () -> {
            int cursor = 0;
            while (!dataflow.isStopped() || cursor < dataflow.getLogList().size()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (cursor < dataflow.getLogList().size()) {
                    EtlLog etlLog = dataflow.getLogList().get(cursor);
                    if (etlLog == null) {
                        log.error("日志为空");
                    }
                    handler.accept(etlLog);
                    cursor++;
                }
            }

        };
        new Thread(runnable, "LogListener-" + dataflow.getId()).start();
    }

    public static List<Tuple2<Node, Pipe>> getAllNodesAndPipes(Dataflow dataflow) {
        List<Tuple2<Node, Pipe>> results = new ArrayList<>();
        recurNodes(dataflow.getHead(), results::add);
        return results;
    }

    public static Node findNodeById(Dataflow dataflow, String id) {
        AtomicReference<Node> re = new AtomicReference<>();
        forEachNodesOrPipes(dataflow, (node, pipe) -> {
            if (node != null && id.equals(node.getId())) {
                re.set(node);
                return false;
            }
            return true;
        });

        return re.get();
    }

    public static void forEachNodesOrPipes(Dataflow dataflow, BiFunction<Node, Pipe, Boolean> consumer) {
        recurNodes(dataflow.getHead(), nodePipeTuple2 -> consumer.apply(nodePipeTuple2.getPartA(), nodePipeTuple2.getPartB()));
    }


    private static void recurNodes(Object currentNode, Function<Tuple2<Node, Pipe>, Boolean> handler) {
        if (currentNode instanceof Node) {
            Node node = (Node) currentNode;
            boolean isContinue = handler.apply(new Tuple2<>(node, null));
            if (!isContinue) {
                return;
            }
            if (!node.getAfterPipes().isEmpty()) {
                for (Pipe afterPipe : node.getAfterPipes()) {
                    recurNodes(afterPipe, handler);
                }
            }
        } else if (currentNode instanceof Pipe) {
            Pipe pipe = (Pipe) currentNode;
            boolean isContinue = handler.apply(new Tuple2<>(null, pipe));
            if (!isContinue) {
                return;
            }
            if (pipe.getTo().isPresent()) {
                recurNodes(pipe.getTo().get(), handler);
            }
        }
    }

}
