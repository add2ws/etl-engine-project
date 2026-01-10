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
public class PGToClickhouse {

    public static void main(String[] args) {

        //创建Postgres数据源(DataSourceUtil代码已省略)
        DataSource dataSourcePG = DataSourceUtil.getOracleDataSource();
        //创建表输入节点
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourcePG, "select * from t_resident_info");

        //创建Clickhouse数据源
        DataSource dataSourceCH = DataSourceUtil.getClickhouseDataSource();
        //创建插入/更新节点
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourceCH, "t_resident_info", 1000);
        //设置唯一标识(主键)映射，用于判断 Insert 或 Update
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID", "ID")));

        //创建管道，并设定缓冲区为1000条数据
        Pipe pipe = new Pipe(1000);
        //连接表输入和输出节点
        pipe.connect(sqlInputNode, upsertOutputNode);

        //创建数据流实例
        Dataflow dataflow = new Dataflow(sqlInputNode);
        //启动数据流，并设定5分钟后超时
        dataflow.syncStart(5, TimeUnit.MINUTES);
    }
}