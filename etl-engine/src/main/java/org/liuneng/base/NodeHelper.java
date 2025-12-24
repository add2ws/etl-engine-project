package org.liuneng.base;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

@Slf4j
public class NodeHelper {

    private final Node NODE;

    private NodeHelper(Node node) {
        this.NODE = node;
    }

    public static NodeHelper of(Node node) {
        return new NodeHelper(node);
    }

    public String[] getUpstreamColumns(InputNode inputNode) {
        Set<String> columns = new LinkedHashSet<>();
        InputNode current = inputNode;
        do {
            columns.addAll(Arrays.asList(current.getInputColumns()));
            current = current.asNode().getPreviousNode().orElse(null);
        } while (current != null);

        return columns.toArray(new String[0]);
    }
}
