package org.liuneng.nodeextension;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.liuneng.base.*;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.exception.NodeWritingException;
import org.liuneng.util.DBUtil;
import org.liuneng.base.NodeHelper;
import org.liuneng.util.StrUtil;
import org.liuneng.util.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class InsertOutputNode extends Node implements OutputNode, DataProcessingMonitor {
    final static Logger log = LoggerFactory.getLogger(InsertOutputNode.class);

    private String[] columns;

    private JdbcTemplate jdbcTemplate;

    @Getter @Setter
    private DataSource dataSource;
    @Getter @Setter
    private String table;
    @Getter @Setter
    private String deleteSql;
    @Setter
    private boolean isDeleted = false;
    @Getter @Setter
    private int batchSize;

    private final List<Tuple2<String, String>> columnsMapping = new ArrayList<>();

    private final List<Row> batchData = new ArrayList<>();

    private PreparedStatement currentPreparedStatement;
    @Getter @Setter
    private long processed;
    @Getter @Setter
    private long processingRate;
    @Setter
    private long startTime;

    @Override
    public void write(@NonNull Row row) throws NodeWritingException {
        if (!isDeleted && StrUtil.isNotBlank(deleteSql)) {
            jdbcTemplate.update(deleteSql);
            isDeleted = true;
            log.info("{} 删除表数据成功", this.getId());
        }
        if (!row.isEnd()) {
            batchData.add(row);
        } else {
            commitBatch();
            super.writeInfoLog(String.format("InsertOutputNode[%s] completed, processed=%d, time consuming=%ds.", this.getName(), processed, (System.currentTimeMillis() - startTime)/1000));
        }

        if (batchData.size() == batchSize) {
            commitBatch();
            batchData.clear();
        }
    }

    private String insertSql = null;
    private void commitBatch() {
        if (batchData.isEmpty()) return;

        long startTime = System.currentTimeMillis();
        if (insertSql == null) {
            String columnsSql = columnsMapping.stream().map(Tuple2::getPartB).collect(Collectors.joining(","));
            String valuesSql = String.join(",", Collections.nCopies(columnsMapping.size(), "?"));
            insertSql = String.format("insert into %s (%s)values(%s)", table, columnsSql, valuesSql);
        }
        jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int indexOfBatch) throws SQLException {
                currentPreparedStatement = preparedStatement;
                int i = 1;
                for (Tuple2<String, String> colMap : columnsMapping) {
                    Object value = batchData.get(indexOfBatch).get(colMap.getPartA());
                    preparedStatement.setObject(i++, value);
                }
            }

            @Override
            public int getBatchSize() {
                return batchData.size();
            }
        });

        long elapsedMillis = System.currentTimeMillis() - startTime;
        if (elapsedMillis == 0) {
            processingRate = -1;
        } else {
            processingRate = (long) (1.0 * batchSize / elapsedMillis * 1000);
        }
        log.info("提交成功！，总量={}条 速度={}条/秒", batchData.size(), processingRate);
        processed += batchData.size();
    }


    public List<Tuple2<String, String>> getColumnMapping() {
        return this.columnsMapping;
    }

    public void setColumnMapping(List<Tuple2<String, String>> columnsMapping) {
        this.columnsMapping.clear();
        this.columnsMapping.addAll(columnsMapping);
    }


    public List<Tuple2<String, String>> autoMapTargetColumns() throws Exception {
        log.info("{} 开始自动匹配列。。。。。。", this.getId());
//        InputNode from = this.getPrevPipe().orElseThrow(() -> new Exception("无法获得上一节点的列信息")).from().orElseThrow(() -> new Exception("无法获得上一节点的列信息"));
        String[] sourceColumns = NodeHelper.of(this).getUpstreamColumns();
        this.columnsMapping.clear();
        for (String targetColumn : this.getTableColumns()) {
            for (String sourceColumn : sourceColumns) {
                if (sourceColumn.equalsIgnoreCase(targetColumn)) {
                    this.columnsMapping.add(new Tuple2<>(sourceColumn, targetColumn));
                    break;
                }
            }
        }
        if (this.columnsMapping.isEmpty()) {
            throw new Exception("自动匹配列名失败！没有一个列能匹配上。");
        }
        log.info("{} 自动匹配列完成", this.getId());
        return columnsMapping;
    }

    public InsertOutputNode() {

    }


    public InsertOutputNode(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.batchSize = 100;
        this.table = table;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public InsertOutputNode(DataSource dataSource, String table, int batchSize) {
        this.dataSource = dataSource;
        this.batchSize = batchSize;
        this.table = table;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public void ping() {
    }


//    @Override
    public String[] getTableColumns() {
        if (columns == null) {
            try {
                columns = DBUtil.lookupColumns(dataSource, table);
            } catch (SQLException e) {
                throw new NodeException(e);
            }
        }
        return columns;
    }

    @Override
    protected void prestart(Dataflow dataflow) throws NodePrestartException {
        startTime = System.currentTimeMillis();
        super.prestart(dataflow);
        if (columnsMapping.isEmpty()) {
            try {
                autoMapTargetColumns();
            } catch (Exception e) {
                throw new NodePrestartException(e);
            }
        }
    }

    @Override
    protected void onDataflowStop() {
        try {
            if (currentPreparedStatement != null && !currentPreparedStatement.isClosed()) {
                currentPreparedStatement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }



    @Override
    public long getStartTime() {
        return 0;
    }

    @Override
    public long getInserted() {
        return processed;
    }

    @Override
    public long getInsertingRate() {
        return processingRate;
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
