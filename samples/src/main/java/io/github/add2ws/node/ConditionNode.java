package io.github.add2ws.node;

import lombok.NonNull;
import org.liuneng.base.MiddleNode;
import org.liuneng.base.Row;
import org.liuneng.exception.NodeException;

public class ConditionNode extends MiddleNode {


    @Override
    public @NonNull Type getType() {
        return Type.SWITCH;
    }

    @Override
    protected @NonNull Row process(@NonNull Row row) throws NodeException {

        Object gender = row.get("gender");
        if ("1".equals(gender)) {
            row.setPipeIndex(0);
            return row;
        } else {
            row.setPipeIndex(1);
            return row;
        }
    }

    @Override
    public String[] getColumns() throws NodeException {
        return new String[0];
    }
}
