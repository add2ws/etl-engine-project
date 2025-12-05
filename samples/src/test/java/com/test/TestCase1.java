package com.test;

import io.github.add2ws.util.DataSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.liuneng.base.Dataflow;
import org.liuneng.base.Pipe;
import org.liuneng.node.SqlInputNode;
import org.liuneng.node.UpsertOutputNode;
import org.liuneng.util.DataflowHelper;
import org.liuneng.util.Tuple2;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TestCase1 {

    @Test
    void oracleToPG() {
        DataSource dataSourceOracle = DataSourceUtil.getOracleDataSource();
        DataSource dataSourcePG = DataSourceUtil.getPostgresDataSource();

        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceOracle, "select * from t_resident_info_2025_10");
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info", 1000);
        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("xh", "xh")));
        Pipe pipe = new Pipe(1000);
        pipe.connect(sqlInputNode, upsertOutputNode);

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

        try {
            Thread.sleep(1000 * 60);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
//        String sql = "SELECT xh::text, ywbh, llywlb, llywlbbm, sjywlb, yywbh, jyzt, jyywzmzsmc, jyywzmzsmcbm, jyywzmzsh, djywzmzsmc, djywzmzsmcbm, djywzmzsh, jyzlb, jyzlbbm, jyzqc, jyzzjmc, jyzzjmcbm, jyzzjhm, jyzxz, jyzxzbm, jyzhj, jyzhjxzqh, bdcdyh, fwbm, fwzt, xzqhdm, qx, xzjdb, jjxzqh, jjxzqhdm, ljx, xq, lh, szqsc, szzzc, myc, dy, fh, fwzl, hxjs, hxjsbm, hxjg, hxjgbm, fwcx, fwcxbm, jzmj, tnjzmj, gtjzmj, ghyt, jzjg, jzjgbm, fwyt, fwytbm, fwxz, fwxzbm, fwlx, fwlxbm, gyfs, gyfsbm, szfe, cjje, fklx, fklxbm, dkfs, dkfsbm, htsxrq, ywbjsj, ywbjrsfzh, ywblszxzqhdm, cityno, zlsj, zlid, del_status, insert_time, jfrq, sfk, ysxkzh, ybdczh, xhs\n" +
//                "FROM t_resident_info where 1=1 " +
//                "and substr(ywbjsj, 1, 10) > '2025-11-02'" +
//                "limit 124";

        SqlInputNode sqlInputNode = new SqlInputNode(dataSourceDuck, sql);
        sqlInputNode.setFetchSize(1000);
        UpsertOutputNode upsertOutputNode = new UpsertOutputNode(dataSourcePG, "t_resident_info");
        upsertOutputNode.setFindMatchInjection("xh=555555");
//        upsertOutputNode.setIdentityMapping(Arrays.asList(new Tuple2<>("xh", "xh")));

        Pipe pipe = new Pipe(10000);
        pipe.connect(sqlInputNode, upsertOutputNode);

        Dataflow dataflow = new Dataflow(sqlInputNode);
//        dataflow.setProcessingThresholdLog(100);
        DataflowHelper.logListener(dataflow, etlLog -> {
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
        ArrayList<Object> list = new ArrayList<>();
        Object object = list.get(3);
        System.out.println(object);
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