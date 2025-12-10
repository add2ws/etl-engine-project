package io.github.add2ws;

import io.github.add2ws.util.DataSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.Dataflow;
import org.liuneng.base.Pipe;
import org.liuneng.node.InsertOutputNode;
import org.liuneng.node.SqlInputNode;
import org.liuneng.node.UpsertOutputNode;
import org.liuneng.util.DataflowHelper;
import org.liuneng.util.Tuple2;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OracleToPG {

    static void oracleToPG() {
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSourcePool();
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSourcePool();

        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, "select * from etl_base.t_resident_info where 1=1 and rownum<= 200000", 1000);
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        upsertOutputNode.setInsertOnly(true);
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));
        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, upsertOutputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.setProcessingThresholdLog(100);
//        DataflowHelper.logListener(dataflow, etlLog -> {
//            System.out.println(etlLog.getMessage());
//        });
        try {
            dataflow.syncStart(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    static void oracleToPGInsert() {
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSourcePool();
        log.info("使用连接池。。。。。。。。。。。。。。。。。。。。。。。。。。");

        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, "select * from etl_base.t_resident_info where 1=1 and rownum<= 200000", 1000);
        InsertOutputNode outputNode = new InsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, outputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.setProcessingThresholdLog(5000);
        DataflowHelper.logListener(dataflow, etlLog -> {
            System.out.println(etlLog.getMessage());
        });
        try {
            dataflow.syncStart(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        oracleToPGInsert();

        try {
            Thread.sleep(1000 * 60 * 100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
