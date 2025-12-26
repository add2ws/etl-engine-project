package org.liuneng.base;

import lombok.NonNull;
import org.liuneng.exception.NodeException;

public interface MiddleNode {

    enum Type {
        COPY, SWITCH
    }

    enum FailedPolicy {
        TERMINATE_DATAFLOW,
        END_DOWNSTREAM
    }

    @NonNull
    Row process(@NonNull Row row) throws NodeException;

    default Type getType() {
        return Type.COPY;
    }

    default FailedPolicy getFailedPolicy() {
        return FailedPolicy.TERMINATE_DATAFLOW;
    }

    default Node asNode() {
        return (Node) this;
    }
}