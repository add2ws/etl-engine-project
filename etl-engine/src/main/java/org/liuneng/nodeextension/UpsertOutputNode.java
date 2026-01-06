package org.liuneng.nodeextension;

import cn.hutool.json.JSONUtil;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.liuneng.base.*;
import org.liuneng.exception.NodeException;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.exception.NodeWritingException;
import org.liuneng.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class UpsertOutputNode extends Node implements OutputNode, DataProcessingMonitor {
    final static Logger log = LoggerFactory.getLogger(UpsertOutputNode.class);

    private String[] columns;

    @Getter
    @Setter
    private DataSource dataSource;

    private Connection connection;

    @Getter
    @Setter
    private String table;

    @Getter
    @Setter
    private String findMatchInjection;

    @Getter
    @Setter
    private String updateInjection;

    @Getter
    @Setter
    private boolean isIgnoreCase = true;

    @Getter
    @Setter
    private boolean insertOnly;

    @Getter
    @Setter
    private int batchSize;

    private final List<Tuple3<String, String, UpsertTag>> columnsMapping = new ArrayList<>();//targetColumn map sourceColumn

    private final List<Tuple2<String, String>> identityMapping = new ArrayList<>();//targetColumn map sourceColumn

    private final List<Row> batchData = new ArrayList<>();

    @Getter
    private long processed = 0;

    @Getter
    private long processingRate = 0;

    @Getter
    private long inserted = 0;

    @Getter
    private long insertingRate = 0;

    @Getter
    private long updated = 0;

    @Getter
    private long updatingRate = 0;

    @Getter
    private long startTime;

    @Override
    public void write(@NonNull Row row) throws NodeWritingException {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        if (row.isEnd()) {
            commitBatch();
            batchData.clear();
            super.writeInfoLog(String.format("UpsertOutputNode[%s] completed, processed(inserted/updated)=%d(%d/%d), time consuming=%ds.", this.getName(), processed, inserted, updated, (System.currentTimeMillis() - startTime) / 1000));
            return;
        }

        if (batchData.size() < batchSize) {
            batchData.add(row);
        }

        if (batchData.size() == batchSize) {
            commitBatch();
            batchData.clear();
        }
    }

    private void commitBatch() {
        if (batchData.isEmpty()) return;

        long startTime = System.currentTimeMillis();
        int inserted = 0;
        int updated = 0;

        if (!this.identityMapping.isEmpty() || StrUtil.isNotBlank(findMatchInjection) ) {//判断主键映射是否为空，为空则全部insert
            List<Map<String, Object>> matchedRows;
            try {
                matchedRows = retrieveMatchRows();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
                throw new NodeWritingException("Finding match rows error: " + e.getMessage());
            }
            log.debug("查询出来了 {} 条", matchedRows.size());

            Tuple2<List<Row>, List<Row>> willInsertAndUpdateRows = findWillInsertAndUpdateRows(matchedRows);

            try {
                inserted = insertBatch(willInsertAndUpdateRows.getPartA());
            } catch (SQLException e) {
                log.debug(e.getMessage(), e);
                throw new NodeWritingException("Insert rows error: " + e.getMessage());
            }
            try {
                updated = updateBatch(willInsertAndUpdateRows.getPartB());
            } catch (SQLException e) {
                log.debug(e.getMessage(), e);
                throw new NodeWritingException("Update rows error: " + e.getMessage());
            }
        } else {
            try {
                inserted = insertBatch(batchData);
            } catch (SQLException e) {
                log.debug(e.getMessage(), e);
                throw new NodeWritingException("Insert rows error: " + e.getMessage());
            }
        }


        this.inserted += inserted;
        this.updated += updated;
        this.processed += batchData.size();

        long currentTimeMillis = System.currentTimeMillis();
        double elapsedSeconds = (currentTimeMillis - startTime) / 1000.0;
        insertingRate = (long) (inserted / elapsedSeconds);
        updatingRate = (long) (updated / elapsedSeconds);
        processingRate = (long) (batchData.size() / elapsedSeconds);
        log.info("提交成功！，总量(插入/更新)={}({}/{}) 速度={}条/秒", batchData.size(), inserted, updated, this.processingRate);
    }

//    private PreparedStatement findMatchRowsPreparedstatement = null;

    private String findMatchSqlBase = null;
    private String findMatchSqlPredicateItem = null;

    /**
     * 从目标库中查询匹配的数据
     *
     * @return
     * @throws SQLException
     */
    private List<Map<String, Object>> retrieveMatchRows() throws SQLException {
        if (findMatchSqlBase == null) {
            findMatchSqlBase = String.format("select %s from %s",
                    columnsMapping.stream().map(cp -> adaptStringCase(cp.getPartB())).collect(Collectors.joining(",")),
                    adaptStringCase(table));

            findMatchSqlPredicateItem = identityMapping.stream().map(identityPair -> String.format("%s=?", adaptStringCase(identityPair.getPartB())))
                    .collect(Collectors.joining(" and "));
            if (StrUtil.isNotBlank(findMatchInjection)) {
                if (identityMapping.isEmpty()) {
                    findMatchSqlPredicateItem = findMatchInjection;
                } else {
                    findMatchSqlPredicateItem += " and " + findMatchInjection;
                }
            }
        }
        String predicates = String.join(" or ", Collections.nCopies(batchData.size(), "(" + findMatchSqlPredicateItem + ")"));
        String sql = findMatchSqlBase + " where " + predicates;
        log.debug("Find match rows sql: {}", sql);

        long startTime = System.currentTimeMillis();
        log.info("准备检索匹配数据，待匹配条数: {}", batchData.size());

        try (PreparedStatement findMatchRowsPreparedstatement = connection.prepareStatement(sql)) {
            int argIndex = 1;
            for (Row row : batchData) {
                for (Tuple2<String, String> inputMapOutput : identityMapping) {
                    findMatchRowsPreparedstatement.setObject(argIndex++, row.get(inputMapOutput.getPartA()));
                }
            }

            List<Map<String, Object>> result = new ArrayList<>();
            ResultSet rs = findMatchRowsPreparedstatement.executeQuery();
            while (rs.next()) {
                Map<String, Object> mapRow = DBUtil.mapRow(rs);
                result.add(mapRow);
            }
            log.info("匹配数据检索完成，共检索出{}条数据，耗时: {} ms", result.size(), System.currentTimeMillis() - startTime);
            return result;
        }

    }

    /**
     * 比对数据是否有差异
     *
     * @param rows
     * @return
     */
    private Tuple2<List<Row>, List<Row>> findWillInsertAndUpdateRows(List<Map<String, Object>> rows) {
        List<Row> willInsert = new ArrayList<>(batchData);
        List<Row> willUpdate = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        log.debug("准备比对目标库数据，筛选待插入和待更新的数据，待比对条数: {}", rows.size());
        for (Row inputRow : batchData) {
            for (Map<String, Object> outputRowData : rows) {
                boolean isFound = identityMapping.stream().allMatch(identityMappingPair -> {
                    Object inputValue = inputRow.get(identityMappingPair.getPartA());
                    Object outputValue = outputRowData.get(identityMappingPair.getPartB());
                    return StrUtil.isEqual(inputValue, outputValue);
                });

                if (isFound) {
                    willInsert.remove(inputRow);
                    if (insertOnly) {
                        break;
                    }

                    if (!isSameRow(inputRow.getMap(), outputRowData)) {
                        willUpdate.add(inputRow);
                        break;
                    }
                }
            }
        }
        log.debug("目标库数据比对结束，筛选出待插入 {} 条，待更新 {} 条，耗时：{} ms", willInsert.size(), willUpdate.size(), System.currentTimeMillis() - startTime);

        return new Tuple2<>(willInsert, willUpdate);
    }

    private boolean isSameRow(Map<String, Object> inputRow, Map<String, Object> outputRow) {
        for (Tuple3<String, String, UpsertTag> columnMap : columnsMapping) {
            if (columnMap.getPartC() == UpsertTag.UPDATE_ONLY) {
                continue;
            }

            Object inputValue = inputRow.get(columnMap.getPartA());
            Object outputValue = outputRow.get(columnMap.getPartB());
            if (!StrUtil.isEqual(inputValue, outputValue)) {
                return false;
            }
        }

        return true;
    }

    private PreparedStatement insertPreparedStatement = null;

    private int insertBatch(List<Row> rows) throws SQLException {
        if (rows.isEmpty()) {
            return 0;
        }
        if (insertPreparedStatement == null) {
            String columnsSql = columnsMapping.stream().map(cp -> adaptStringCase(cp.getPartB())).collect(Collectors.joining(","));
            String valuesSql = String.join(",", Collections.nCopies(columnsMapping.size(), "?"));
            String insertSql = String.format("insert into %s (%s)values(%s)", table, columnsSql, valuesSql);
            insertPreparedStatement = connection.prepareStatement(insertSql);
            log.debug("Insert sql: {}", insertSql);
        }

        long startTime = System.currentTimeMillis();
        log.info("待插入数据：{}条", rows.size());
        for (Row row : rows) {
            for (int i = 0; i < columnsMapping.size(); i++) {
                Object obj = row.get(columnsMapping.get(i).getPartA());
                insertPreparedStatement.setObject(i + 1, obj);
            }
            insertPreparedStatement.addBatch();
        }
        int[] inserted = insertPreparedStatement.executeBatch();
        log.info("数据插入完成，耗时：{} ms", System.currentTimeMillis() - startTime);

        return inserted.length;
    }

    private PreparedStatement updatePreparedStatement = null;

    private int updateBatch(List<Row> rows) throws SQLException {
        if (rows.isEmpty()) {
            return 0;
        }
        if (updatePreparedStatement == null) {
            String setClause = columnsMapping.stream().filter(mapping -> mapping.getPartC() != UpsertTag.COMPARE_ONLY)
                    .map(cp -> adaptStringCase(cp.getPartB()) + "=?")
                    .collect(Collectors.joining(", "));
            if (StrUtil.isNotBlank(updateInjection)) {
                setClause += ", ".concat(updateInjection);
            }
            String updateSql = String.format("update %s set %s where ", adaptStringCase(table), setClause);
            updateSql += identityMapping.stream().map(identityPair -> String.format("%s=?", adaptStringCase(identityPair.getPartB()))).collect(Collectors.joining(" and "));
            updatePreparedStatement = connection.prepareStatement(updateSql);
            log.debug("Update sql: {}", updateSql);
        }
        long startTime = System.currentTimeMillis();
        log.debug("待更新数据：{}条", rows.size());
        for (Row row : rows) {
            int i = 1;
            for (Tuple3<String, String, UpsertTag> colMap : columnsMapping) {
                Object val = row.get(colMap.getPartA());
                updatePreparedStatement.setObject(i++, val);
            }
            for (Tuple2<String, String> pair : identityMapping) {
                updatePreparedStatement.setObject(i++, row.get(pair.getPartA()));
            }
            updatePreparedStatement.addBatch();
        }

        int[] updated = updatePreparedStatement.executeBatch();
        log.debug("数据更新完成，耗时：{} ms", System.currentTimeMillis() - startTime);
        return updated.length;
    }

    public List<Tuple3<String, String, UpsertTag>> getColumnMapping() {
        return this.columnsMapping;
    }

    public void setColumnMapping(List<Tuple3<String, String, UpsertTag>> columnsMapping) {
        this.columnsMapping.clear();
        this.columnsMapping.addAll(columnsMapping);
    }


    public List<Tuple3<String, String, UpsertTag>> autoMapTargetColumns() {
        log.info("Start auto map target columns...");
        long time = System.currentTimeMillis();
//        InputNode from = this.getPrevPipe().orElseThrow(() -> new NodeException("无法获得上一节点的列信息")).from().orElseThrow(() -> new NodeException("无法获得上一节点的列信息"));
        String[] sourceColumns = NodeHelper.of(this).getUpstreamColumns();

        this.columnsMapping.clear();
        for (String targetColumn : this.getTableColumns()) {
            for (String sourceColumn : sourceColumns) {
                if (sourceColumn.equalsIgnoreCase(targetColumn)) {
                    this.columnsMapping.add(new Tuple3<>(sourceColumn, targetColumn, UpsertTag.COMPARE_AND_UPDATE));
                    break;
                }
            }
        }
        if (this.columnsMapping.isEmpty()) {
            throw new NodeException("Auto map failed! No columns are matched.");
        }
        log.info("Auto map completed，elapsed {}ms", System.currentTimeMillis() - time);
        log.debug(JSONUtil.toJsonPrettyStr(this.columnsMapping));
        return this.columnsMapping;
    }

    public UpsertOutputNode() {

    }

    public UpsertOutputNode(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.batchSize = 100;
        this.table = table;
        this.updateInjection = "";
    }

    public UpsertOutputNode(DataSource dataSource, String table, int batchSize) {
        this.dataSource = dataSource;
        this.batchSize = batchSize;
        this.table = table;
        this.updateInjection = "";
    }

    public UpsertOutputNode(DataSource dataSource, String table, int batchSize, String updateInjection) {
        this.dataSource = dataSource;
        this.batchSize = batchSize;
        this.table = table;
        this.updateInjection = updateInjection;
    }

    public List<Tuple2<String, String>> getIdentityMapping() {
        return identityMapping;
    }

    public void setIdentityMapping(List<Tuple2<String, String>> identityMapping) {
        this.identityMapping.clear();
        this.identityMapping.addAll(identityMapping);
    }

    public void addIdentityMapping(Tuple2<String, String> identityPair) {
        this.identityMapping.add(identityPair);
    }


    public String[] getTableColumns() throws NodeException {
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
        super.prestart(dataflow);
        try {
            connection = dataSource.getConnection();
//        connection.setAutoCommit(false);
            if (columnsMapping.isEmpty()) {
                autoMapTargetColumns();
            }
        } catch (SQLException e) {
            throw new NodePrestartException(e);
        }

    }

    @Override
    protected void onDataflowStop() {
        try {
//            if (findMatchRowsPreparedstatement != null && !findMatchRowsPreparedstatement.isClosed()) {
//                findMatchRowsPreparedstatement.cancel();
//                findMatchRowsPreparedstatement.close();
//            }

            if (insertPreparedStatement != null && !insertPreparedStatement.isClosed()) {
                insertPreparedStatement.cancel();
                insertPreparedStatement.close();
            }

            if (updatePreparedStatement != null && !updatePreparedStatement.isClosed()) {
                updatePreparedStatement.cancel();
                updatePreparedStatement.close();
            }

            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private String adaptStringCase(String src) {
        if (isIgnoreCase) {
            return src;
        }
        return String.format("\"%s\"", src);
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
