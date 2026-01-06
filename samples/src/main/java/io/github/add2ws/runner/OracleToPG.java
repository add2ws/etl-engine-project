package io.github.add2ws.runner;

import io.github.add2ws.util.DataSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.liuneng.base.Dataflow;
import org.liuneng.base.Pipe;
import org.liuneng.nodeextension.SqlInputNode;
import org.liuneng.nodeextension.UpsertOutputNode;
import org.liuneng.util.Tuple2;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OracleToPG {

    public static void main(String[] args) {


        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        String sql = "SELECT * FROM ETL_BASE.T_RESIDENT_INFO WHERE 1=1 AND ROWNUM < 200000";
//        String sql = "select * from (select * from ETL_BASE.T_RESIDENT_INFO where 1=1 order by ID) T where ROWNUM < 200000";
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, sql);

        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        upsertOutputNode.setInsertOnly(true);
//        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));

        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, upsertOutputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.setProcessingThresholdLog(10000);
        dataflow.syncStart(5, TimeUnit.MINUTES);
    }
}