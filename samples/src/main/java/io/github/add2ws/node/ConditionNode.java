package io.github.add2ws.node;

import lombok.NonNull;
import org.liuneng.base.MiddleNode;
import org.liuneng.base.Row;
import org.liuneng.exception.NodeException;

public class ConditionNode extends MiddleNode {


    @Override
    protected @NonNull Row process(@NonNull Row row) throws NodeException {

        Object gender = row.get("gender");
        if ("1".equals(gender)) {
            // 将gender=1的数据分发到第1个后续管道
            row.setPipeIndex(0);
            return row;
        } else {
            // 否则分发到第2个后续管道
            row.setPipeIndex(1);
            return row;
        }
    }

    @Override
    public String[] getColumns() throws NodeException {
        return new String[0];
    }

    @Override
    public @NonNull Type getType() {
        //中间节点类型：当后面连接多个节点时，对途径的数据流是拷贝还是分发
        return Type.SWITCH;
    }
}
