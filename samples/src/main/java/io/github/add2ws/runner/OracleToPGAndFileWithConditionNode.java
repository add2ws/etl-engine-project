package io.github.add2ws.runner;

import io.github.add2ws.node.ConditionNode;
import io.github.add2ws.util.DataSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.Dataflow;
import org.liuneng.base.Pipe;
import org.liuneng.nodeextension.FileOutputNode;
import org.liuneng.nodeextension.SqlInputNode;
import org.liuneng.nodeextension.UpsertOutputNode;
import org.liuneng.util.Tuple2;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OracleToPGAndFileWithConditionNode {

    public static void main(String[] args) {

        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        String sql = "SELECT * FROM ETL_BASE.T_RESIDENT_INFO WHERE 1=1 AND ROWNUM < 50000";
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, sql);

        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));

        ConditionNode conditionNode = new ConditionNode();

        FileOutputNode fileOutputNode = new FileOutputNode("D:/t_resident_info_female_" + System.currentTimeMillis() +".csv", FileOutputNode.Format.CSV);

//        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));
        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, conditionNode);

        pipe = new Pipe(10000);
        pipe.connect(conditionNode, upsertOutputNode);

        pipe = new Pipe(10000);
        pipe.connect(conditionNode, fileOutputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.setProcessingThresholdLog(10000);
        dataflow.syncStart(5, TimeUnit.MINUTES);
    }
}