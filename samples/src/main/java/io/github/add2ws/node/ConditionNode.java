package io.github.add2ws.node;

import lombok.NonNull;
import org.liuneng.base.MiddleNode;
import org.liuneng.base.Node;
import org.liuneng.base.Row;
import org.liuneng.exception.NodeException;

public class ConditionNode extends Node implements MiddleNode {

    @Override
    public Type getType() {
        return Type.SWITCH;
    }

    @Override
    public @NonNull Row process(@NonNull Row row) throws NodeException {

        if (row.get("gender").equals("1")) {
            row.setPipeIndex(0);
            return row;
        } else {
            row.setPipeIndex(1);
            return row;
        }
    }

    @Override
    protected void onDataflowStop() {

    }
}
