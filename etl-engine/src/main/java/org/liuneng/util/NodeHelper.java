package org.liuneng.util;

import org.liuneng.base.InputNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class NodeHelper {

    private final static Logger log = LoggerFactory.getLogger(NodeHelper.class);

    public static String[] getUpstreamColumns(InputNode inputNode) throws Exception {
        Set<String> columns = new LinkedHashSet<>();
        InputNode current = inputNode;
        do {
            columns.addAll(Arrays.asList(current.getInputColumns()));
            current = current.asNode().getBeforeNode().orElse(null);
        } while (current != null);

        return columns.toArray(new String[0]);
    }
}
