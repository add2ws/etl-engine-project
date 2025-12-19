package org.liuneng.util;

import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.InputNode;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

@Slf4j
public class NodeHelper {

    

    public static String[] getUpstreamColumns(InputNode inputNode) {
        Set<String> columns = new LinkedHashSet<>();
        InputNode current = inputNode;
        do {
            columns.addAll(Arrays.asList(current.getInputColumns()));
            current = current.asNode().getPreviousNode().orElse(null);
        } while (current != null);

        return columns.toArray(new String[0]);
    }
}
