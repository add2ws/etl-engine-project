package org.liuneng.base.monitor;

import org.liuneng.base.Dataflow;

public class DataflowMonitor {

    private Dataflow dataflowInstance;

    private DataflowMonitor() {

    }

    public static DataflowMonitor watch(Dataflow dataflow) {
        DataflowMonitor dataflowMonitor = new DataflowMonitor();
        dataflowMonitor.setDataflowInstance(dataflow);
        
        return dataflowMonitor;
    }


    public Dataflow getDataflowInstance() {
        return dataflowInstance;
    }

    public void setDataflowInstance(Dataflow dataflowInstance) {
        this.dataflowInstance = dataflowInstance;
    }
}
