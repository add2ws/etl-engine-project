package io.github.add2ws.node;

import lombok.NonNull;
import org.liuneng.base.InputNode;
import org.liuneng.base.Node;
import org.liuneng.base.Row;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodeReadingException;

public class HttpInputNode extends Node implements InputNode {


    @Override
    public @NonNull Row read() throws NodeReadingException {

        return null;
    }

    @Override
    public String[] getColumns() throws NodeException {
        return new String[0];
    }

    @Override
    protected void onDataflowPrestart() {

    }

    @Override
    protected void onDataflowStop() {

    }
}
