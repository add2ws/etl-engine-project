package io.github.add2ws.runner;

import cn.hutool.json.JSONUtil;
import io.github.add2ws.util.DataSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.Dataflow;
import org.liuneng.base.Pipe;
import org.liuneng.nodeextension.FileOutputNode;
import org.liuneng.nodeextension.SqlInputNode;
import org.liuneng.nodeextension.UpsertOutputNode;
import org.liuneng.nodeextension.UpsertTag;
import org.liuneng.base.DataflowHelper;
import org.liuneng.util.Tuple2;
import org.liuneng.util.Tuple3;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SamplesMain {
    final static String src = "";

    public static Dataflow testEtlTask() {
//        String sql = "select * from t_resident_info where 1=1 " +
//                "and substr(ywbjsj, 1, 7) between '2024-01' and '2024-03' " +
//                "and rownum<122283";

        String sql = "select * from t_resident_info where 1=1\n" +
                "and rownum<=5000\n ";

//        DataSource oraclePool = DBUtil.getOracleDataSourcePool();

        DataSource oracle = DataSourceUtil.getOracleDataSource();
        DataSource postgresPool = DataSourceUtil.getPostgresDataSourcePool();
        DataSource postgres = DataSourceUtil.getPostgresDataSource();
        DataSource duckdb = DataSourceUtil.getDuckDBDataSource("E:/duck.db");

        SqlInputNode sqlReaderNode_oracle = new SqlInputNode(oracle, sql, 100);
        FileOutputNode fileWriterNode = new FileOutputNode("D:/表数据.txt", FileOutputNode.Format.JSON);

//        InsertOutputNode insertWriterNode_pg = new InsertOutputNode(postgresPool, "t_resident_info", 200);
//        insertWriterNode_pg.setDeleteSql("truncate table t_resident_info");

//        InsertOutputNode insertWriterNode_oracle = new InsertOutputNode(oraclePool, "t_resident_info_2", 200);
//        insertWriterNode_oracle.setDeleteSql("truncate table t_resident_info_2");

        UpsertOutputNode upsertWriterNode_pg = new UpsertOutputNode(postgresPool, "t_resident_info", 500);
        upsertWriterNode_pg.addIdentityMapping(new Tuple2<>("XH", "XH"));
//        upsertWriterNode_pg.addIdentityMapping(new Tuple2<>("CITYNO", "cityNo"));

        List<Map<String, Object>> columnMaps = JSONUtil.toBean(src, new cn.hutool.core.lang.TypeReference<List<Map<String, Object>>>() {}, true);

        List<Tuple3<String, String, UpsertTag>> columnMapList = new ArrayList<>();
        for (Map<String, Object> columnMap : columnMaps) {
            String ut = columnMap.get("upsertTag").toString();
            UpsertTag upsertTag = null;
            if ("compareOnly".equalsIgnoreCase(ut)) {
                upsertTag = UpsertTag.COMPARE_ONLY;
            } else if ("updateOnly".equalsIgnoreCase(ut)) {
                upsertTag = UpsertTag.UPDATE_ONLY;
            } else if ("compareAndUpdate".equalsIgnoreCase(ut)) {
                upsertTag = UpsertTag.COMPARE_AND_UPDATE;
            }
            columnMapList.add(new Tuple3<>(columnMap.get("from").toString(), columnMap.get("to").toString(), upsertTag));
        }

        UpsertOutputNode upsertWriterNode_oracle = new UpsertOutputNode(oracle, "t_resident_info_2", 200);
        upsertWriterNode_oracle.addIdentityMapping(new Tuple2<>("XH", "XH"));
        upsertWriterNode_oracle.setColumnMapping(columnMapList);
        Pipe pipe = new Pipe(10000);
//        Pipe pipe2 = new Pipe(10000);
//        Pipe pipe3 = new Pipe(10000);

        UpsertOutputNode upsertOutputNodeDuckDB = new UpsertOutputNode(duckdb, "t_resident_info", 100);

        pipe.connect(sqlReaderNode_oracle, upsertOutputNodeDuckDB);

        return new Dataflow(sqlReaderNode_oracle);
    }

    public static void testDataflow() throws InterruptedException {

        Dataflow dataflow = testEtlTask();
        log.info("Dataflow initialized ... ID={}", dataflow.getId());
        DataflowHelper.of(dataflow).logListener(log -> {
            System.out.println("logListener==============>" + log.getMessage());
        });
        dataflow.setProcessingThresholdLog(20000);

        new Thread(() -> {
            try {
//                log.info("after 5 seconds will stop........................................................................");
//                Thread.sleep(5000);
//                log.info("执行手动停止。。。。。{}秒后强制关闭xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 3);
//                dataflow.syncStop(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }).start();

        dataflow.syncStart(3600 * 2, TimeUnit.SECONDS);

        Thread.sleep(2000); //防止日志监听线程被过早关闭
    }

    public static void main(String[] args) {
        try {
            testDataflow();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }
}