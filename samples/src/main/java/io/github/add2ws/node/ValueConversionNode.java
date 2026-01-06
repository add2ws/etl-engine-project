package io.github.add2ws.node;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.MiddleNode;
import org.liuneng.base.Row;
import org.liuneng.exception.NodeException;

@Slf4j
public class ValueConversionNode extends MiddleNode {

    @Override
    protected @NonNull Row process(@NonNull Row row) throws NodeException {
        //将gender列值转换到gender_name列中
        if ("1".equals(row.get("gender"))) {
            row.put("gender_name", "male");
        } else {
            row.put("gender_name", "female");
        }

        //将address列脱敏处理
        String address = String.valueOf(row.get("address"));
        if (address != null) {
            String masked = address.replaceAll("^(.).*(.)$", "$1***$2");
            row.put("address", masked);
        }

        return row;
    }

    @Override
    public String[] getColumns() throws NodeException {
        //新增gender_name列
        return new String[]{"gender_name"};
    }

    @Override
    public @NonNull Type getType() {
        //中间节点类型：当后面连接多个节点时，对途径的数据流是拷贝还是分发
        return Type.COPY;
    }

}
