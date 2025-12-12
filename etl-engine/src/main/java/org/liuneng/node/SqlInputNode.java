package org.liuneng.node;

import lombok.Getter;
import lombok.Setter;
import org.liuneng.base.*;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.exception.NodeReadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SqlInputNode extends Node implements InputNode, DataProcessingMetrics {
    final static Logger log = LoggerFactory.getLogger(SqlInputNode.class);

    @Getter @Setter
    private String charset;

    private String[] columns;
    @Getter @Setter
    private DataSource dataSource;
    @Getter @Setter
    private String sql;
    @Getter @Setter
    private int fetchSize;
    @Getter @Setter
    private long processed;
    @Getter @Setter
    private long processingRate;

    @Getter
    private long startTime;

    private Connection connection;

    private PreparedStatement preparedStatement;

    private ResultSet resultSet;


    @Override
    public Row read() throws NodeReadingException {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        try {
            if (resultSet == null) {
                resultSet = preparedStatement.executeQuery();
                resultSet.setFetchSize(fetchSize);
            }

            long duration = System.currentTimeMillis() - startTime;
            if (resultSet.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (String column : columns) {
                    Object value = resultSet.getObject(column);
                    if (charset != null && value instanceof String) {
                        value = new String(((String) value).getBytes(charset));
                    }
                    row.put(column, value);
                }
                processed ++;
                if (duration > 0) {
                    processingRate = (long) (processed / (duration/1000.0));
                }
                return Row.fromMap(row);
            } else {
                preparedStatement.close();
                resultSet.close();
                connection.close();
                super.dataflowInstance.addInfoLog(String.format("%s completed, processed=%d, time consuming=%ds.", this.getName(), processed, duration /1000));
                return Row.ofEnd();
            }
        } catch (SQLException | UnsupportedEncodingException e) {
            throw new NodeReadingException(e);
        }
    }

    @Override
    public String[] getInputColumns() {
        try {
            if (columns == null) {
                connection = dataSource.getConnection();
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setFetchSize(fetchSize);
                columns = new String[preparedStatement.getMetaData().getColumnCount()];
                for (int i = 0; i < preparedStatement.getMetaData().getColumnCount(); i++) {
                    columns[i] = preparedStatement.getMetaData().getColumnLabel(i + 1);
                }
            }
        } catch (SQLException e) {
            throw new NodeException(e);
        }
        return columns;
    }

    public SqlInputNode() {}

    public SqlInputNode(DataSource dataSource, String sql) {
        this.dataSource = dataSource;
        this.sql = sql;
        this.fetchSize = 0;
    }

    public SqlInputNode(DataSource dataSource, String sql, String charset, int fetchSize) {
        this.dataSource = dataSource;
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.charset = charset;
    }

    public SqlInputNode(DataSource dataSource, String sql, int fetchSize) {
        this.dataSource = dataSource;
        this.sql = sql;
        this.fetchSize = fetchSize;
    }

    @Override
    protected void prestart(Dataflow dataflow) throws NodePrestartException {
        startTime = System.currentTimeMillis();
        log.info("{}[{}] start initializing...", this.getClass().getSimpleName(), super.getId());
        super.prestart(dataflow);
        getInputColumns();
        log.info("{}[{}] has been initialized.", this.getClass().getSimpleName(), super.getId());
    }

    @Override
    protected void onDataflowStop() {
        try {
            if (preparedStatement != null && !preparedStatement.isClosed()) {
                preparedStatement.close();
            }
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
        } catch (SQLException e) {
            log.error("input node closing error!", e);
        }
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
}
