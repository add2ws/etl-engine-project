package org.liuneng.nodeextension;

import lombok.NonNull;
import org.liuneng.base.*;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.exception.NodeWritingException;
import org.liuneng.util.DBUtil;
import org.liuneng.util.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteOutputNode extends Node implements OutputNode, DataProcessingMonitor {
    final static Logger log = LoggerFactory.getLogger(DeleteOutputNode.class);

    private DataSource dataSource;

    private String table;

    private String[] columns;

    private List<Tuple2<String, String>> columnsMapping = new ArrayList<>();//targetColumn map sourceColumn

    private PreparedStatement preparedStatement;

    private final int batchSize;

    private long processed;

    private long processingRate;

    public DeleteOutputNode(DataSource dataSource, String table, int batchSize) {
        this.dataSource = dataSource;
        this.table = table;
        this.batchSize = batchSize;
    }

    public DeleteOutputNode(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.table = table;
        this.batchSize = 100;
    }

    @Override
    public long getProcessed() {
        return processed;
    }

    @Override
    public long getProcessingRate() {
        return processingRate;
    }

    @Override
    public long getInserted() {
        return 0;
    }

    @Override
    public long getInsertingRate() {
        return 0;
    }

    @Override
    public long getUpdated() {
        return 0;
    }

    @Override
    public long getUpdatingRate() {
        return 0;
    }

    @Override
    public long getDeleted() {
        return 0;
    }

    @Override
    public long getDeletingRate() {
        return 0;
    }

    @Override
    public long getStartTime() {
        return dataflowInstance.getStartTime();
    }

    @Override
    public void write(@NonNull Row row) throws NodeWritingException {

    }

    public String[] getTableColumns() throws SQLException {
        if (columns == null) {
            columns = DBUtil.lookupColumns(dataSource, table);
        }
        return columns;
    }

    @Override
    protected void onDataflowPrestart() throws NodePrestartException {
        try {
            this.getTableColumns();
            if (this.columnsMapping.isEmpty()) {
                throw new NodePrestartException("未指定关联字段");
            }

            String whereSql = this.columnsMapping.stream().map(tuple2 -> tuple2.getPartB() + "=?").collect(Collectors.joining(" and "));
            preparedStatement = dataSource.getConnection().prepareStatement(String.format("delete from %s where %s", this.table, whereSql));
        } catch (SQLException | NodePrestartException e) {
            throw new NodePrestartException(e);
        }
    }

    @Override
    protected void onDataflowStop() {
        try {
            if (preparedStatement != null && !preparedStatement.isClosed()) {
                preparedStatement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
