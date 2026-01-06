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
        } else {
            row.put("gender_name", "female");
        }

        String address = String.valueOf(row.get("address"));
        if (address != null) {
            String masked = address.replaceAll("^(.).*(.)$", "$1***$2");
            row.put("address", masked);
        }

        return row;
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
