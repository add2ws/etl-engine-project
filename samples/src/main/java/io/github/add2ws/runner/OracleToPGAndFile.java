package io.github.add2ws.runner;

import io.github.add2ws.node.ConditionNode;
import io.github.add2ws.util.DataSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.Dataflow;
import org.liuneng.base.Pipe;
import org.liuneng.nodeextension.FileOutputNode;
import org.liuneng.nodeextension.SqlInputNode;
import org.liuneng.nodeextension.UpsertOutputNode;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OracleToPGAndFile {

    static void oracleToPGUpsert() {
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        String sql = "SELECT * FROM ETL_BASE.T_RESIDENT_INFO WHERE 1=1 AND ROWNUM < 200000";
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, sql);

        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        upsertOutputNode.setInsertOnly(true);

        ConditionNode conditionNode = new ConditionNode();

        FileOutputNode fileOutputNode = new FileOutputNode(String.format("E:/t_resident_info_female.csv"), FileOutputNode.Format.CSV);

//        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));
        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, upsertOutputNode);

        pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, fileOutputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.setProcessingThresholdLog(10000);
//        DataflowHelper.logListener(dataflow, etlLog -> {
//            System.out.println(etlLog.getMessage());
//        });
        try {
            dataflow.syncStart(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        oracleToPGUpsert();
    }
}