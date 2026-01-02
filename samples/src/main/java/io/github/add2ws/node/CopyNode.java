package io.github.add2ws.node;

import lombok.NonNull;
import org.liuneng.base.MiddleNode;
import org.liuneng.base.Row;
import org.liuneng.exception.NodeException;

public class CopyNode extends MiddleNode {

    @Override
    protected @NonNull Row process(@NonNull Row row) throws NodeException {

        if ("1".equals(row.get("gender"))) {
            row.put("gender_name", "male");
            return row;
        } else {
            row.put("gender_name", "female");
            return row;
        }
    }

    @Override
    public @NonNull Type getType() {
        return Type.COPY;
    }

    @Override
    protected void onDataflowStop() {

    }
}
