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
            if ("1".equals(row.get("gender"))) {
                row.put("gender_name", "male");
                return row;
            } else {
                row.put("gender_name", "female");
                return row;
            }

//        try {
//        } catch (Exception e) {
//            log.error(e.getMessage(), e);
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public @NonNull Type getType() {
        return Type.COPY;
    }

    @Override
    public String[] getColumns() throws NodeException {
        return new String[]{"gender_name"};
    }
}
