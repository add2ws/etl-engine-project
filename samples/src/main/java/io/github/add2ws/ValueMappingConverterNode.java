package io.github.add2ws;

import org.liuneng.base.Row;
import org.liuneng.exception.NodeException;
import org.liuneng.node.ValueConvertNode;

public class ValueMappingConverterNode extends ValueConvertNode {
    @Override
    public Row convert(Row row) {

//        row.getData().get("jyzlb")

        return row;
    }

    @Override
    public long getProcessed() {
        return 0;
    }

    @Override
    public long getProcessingRate() {
        return 0;
    }

    @Override
    public long getStartTime() {
        return 0;
    }

    @Override
    public String[] getOutputColumns() throws NodeException {
        return new String[0];
    }

    @Override
    protected void onDataflowStop() {

    }
}
