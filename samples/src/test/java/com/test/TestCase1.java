package com.test;

import io.github.add2ws.util.DataSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.liuneng.base.Dataflow;
import org.liuneng.base.Pipe;
import org.liuneng.nodeextension.*;
import org.liuneng.base.DataflowHelper;
import org.liuneng.util.Tuple2;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TestCase1 {

    @Test
    void PGToFile() {
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourcePG, "select * from t_resident_info order by xh limit 12333");

        FileOutputNode fileOutputNode = new FileOutputNode("E:/output.csv", FileOutputNode.Format.CSV);

        Pipe pipe = new Pipe(1000);
        pipe.connect(sqlInputNode,fileOutputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);

        dataflow.syncStart();

    }

    @Test
    void oracleToPGAndFile() {
        //创建Oracle数据源
        DataSource oracleDataSource = DataSourceUtil.getOracleDataSource();
        SqlInputNode sqlInputNode = new SqlInputNode(oracleDataSource, "select * from etl_base.t_resident_info where rownum<=50000 order by id");

        //创建Postgres数据源
        DataSource postgresDataSource = DataSourceUtil.getPostgresDataSource();
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(postgresDataSource, "public.t_resident_info", 1000);
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("ID","ID")));

        //创建csv文件目标
        FileOutputNode fileOutputNode = new FileOutputNode("E:/output_" + System.currentTimeMillis() + ".csv", FileOutputNode.Format.CSV);

        //创建管道并连接Oracle和Postgres
        Pipe pipe = new Pipe(1000);
        pipe.connect(sqlInputNode,upsertOutputNode);

        //创建管道并连接Oracle和csv文件
        Pipe pipe_2 = new Pipe(1000);
        pipe_2.connect(sqlInputNode,fileOutputNode);

        //创建数据流并启动
        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.syncStart();

    }

    @Test
    void MysqlAndPG() {
        //获取数据源
        DataSource dataSourceMysql = DataSourceUtil.getMySqlDataSource();
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();

        //创建表输入节点
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourcePG, "select * from t_resident_info limit 20007");

        //创建插入/更新节点
//        UpsertOutputNode outputNode = new UpsertOutputNode(dataSourceMysql, "t_resident_info", 500);
//        outputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("XH", "XH")));

        InsertOutputNode outputNode = new InsertOutputNode(dataSourceMysql, "t_resident_info", 500);


        //创建管道
        Pipe pipe = new Pipe(10000);
        //连接表输入和输出节点
        pipe.connect(sqlInputNode, outputNode);

        //创建数据流实例
        Dataflow dataflow = new Dataflow(sqlInputNode);
        dataflow.setProcessingThresholdLog(10000);
        //启动数据流
        dataflow.syncStart(5, TimeUnit.MINUTES);

        try {
            Thread.sleep(1000 * 60);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void oracleToPG() {
        //创建Oracle数据源
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        //创建表输入节点
        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, "select * from t_resident_info");

        //创建Postgres数据源
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();
        //创建插入/更新节点
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        //设置主键映射
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

    @Test
    void testDuckDB() {
        DataSource dataSourceDuck = DataSourceUtil.getDuckDBDataSource("E:/duck.db");

/*
        try (Connection connection = dataSourceDuck.getConnection()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("delete from main.t_resident_info where substr(ywbjsj, 1, 10) >= '2025-11-01'");
            log.info("删除成功。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
*/


        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();

        String sql = "select * from main.t_resident_info where 1=1 limit 20000 ";

        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceDuck, sql);
        sqlInputNode.setFetchSize(1000);
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info");
        upsertOutputNode.setFindMatchInjection("xh=555555");
//        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("xh", "xh")));

        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, upsertOutputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);
//        dataflow.setProcessingThresholdLog(100);
        DataflowHelper.of(dataflow).logListener( etlLog -> {
            System.out.println(etlLog.getMessage());
        });
        try {
            dataflow.syncStart(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            Thread.sleep(1000 * 60);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void t1() {
        String address = "问";
        String masked = address.replaceAll("^(.).*(.)$", "$1***$2");

        System.out.println(masked);
    }

    @Test
    void testB() {
        BigDecimal number = new BigDecimal("1234567890.000", new MathContext(0));
        number = number.stripTrailingZeros();
        System.out.println(number.toPlainString());
        int precision = number.precision();
        Integer i = 10;

        String string = number.toString();
        String string1 = i.toString();

        System.out.println(string1 + string);
    }

    /*@Test
    void test1() throws InterruptedException {


        ExecutorService executorService = Executors.newCachedThreadPool();
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        ExecutorService executorService = Executors.newFixedThreadPool(5);


        WaitGroup waitGroup = new WaitGroup();
        try {
            waitGroup.add(2);
        } catch (WaitGroup.NegativeCounterException e) {
            throw new RuntimeException(e);
        }

        CountDownLatch countDownLatch = new CountDownLatch(2);


        Future<?> future = executorService.submit(() -> {
            for (int i = 0; i < 5; i++) {
                System.out.println("it is " + (5 - i) + " seconds left");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            countDownLatch.countDown();
        });


        executorService.submit(() -> {
            for (int i = 0; i < 3; i++) {
                System.out.println("it is " + (500 - i) + " seconds left");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            countDownLatch.countDown();
        });

//        boolean await = countDownLatch.await(2, TimeUnit.SECONDS);
        System.out.println("it is done!=>" + 1);

    }*/

    @Test
    void test_wait() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(() -> {
            int i = 3;
            try {
                while (i-- > 0) {
                    log.info("{}秒后终结线程池", i);
                    Thread.sleep(1000);
                }
                executorService.shutdown();
                log.info("已发送终结信号");
                boolean notTimeout = executorService.awaitTermination(2, TimeUnit.SECONDS);
                if (!notTimeout) {
                    log.info("终结超时！准备强制终结");
                    executorService.shutdownNow();
                }

            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            log.info("thread 0 ends.");
        });


        executorService.execute(() -> {
            Boolean b = null;
            try {
                b = executorService.awaitTermination(8, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("thread 1 waited failed!", e);
            }
            boolean isTerminated = executorService.isTerminated();
            log.info("thread 1: is terminated: {}", isTerminated);
            log.info("thread 1 ends....=>{}", b);
        });

        executorService.execute(() -> {
            Boolean b = null;
            try {
                b = executorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("thread 2 waited failed!", e);
            }
            boolean isTerminated = executorService.isTerminated();
            log.info("thread 2: is terminated: {}", isTerminated);
            log.info("thread 2 ends....=>{}", b);
        });

        boolean notTimeout = false;
        try {
            notTimeout = executorService.awaitTermination(30000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        boolean isTerminated = executorService.isTerminated();
        log.info("notTimeout:{} if terminated: {}", notTimeout, isTerminated);
    }
}