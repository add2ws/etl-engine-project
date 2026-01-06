package io.github.add2ws.runner;

import io.github.add2ws.node.ValueConversionNode;
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
public class OracleToPGAndFileWithValueConvertor {

    public static void main(String[] args) {
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        String sql = "SELECT * FROM ETL_BASE.T_RESIDENT_INFO WHERE 1=1 AND ROWNUM <= 10000";
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, sql);

        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));

        ValueConversionNode valueConversionNode = new ValueConversionNode();

        FileOutputNode fileOutputNode = new FileOutputNode("E:/t_resident_info_female_" + System.currentTimeMillis() +".csv", FileOutputNode.Format.CSV);

//        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));
        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, valueConversionNode);

        pipe = new Pipe(10000);
        pipe.connect(valueConversionNode, upsertOutputNode);

        pipe = new Pipe(10000);
        pipe.connect(valueConversionNode, fileOutputNode);

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
}