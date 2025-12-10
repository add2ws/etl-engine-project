package io.github.add2ws.util;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.Properties;

public class DataSourceUtil {

    public static DataSource getOracleDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
        String host = "127.0.0.1";
        int port = 1521;
        String sid = "orcl";
        String url = String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, sid);
        dataSource.setUrl(url);
        dataSource.setUsername("etl_base");
        dataSource.setPassword("etl_base");
        return dataSource;
    }

    public static DataSource getMySqlDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        String host = "127.0.0.1";
        int port = 3308;
        String sid = "etl_base";
        String url = String.format("jdbc:mysql://@%s:%d/%s", host, port, sid);
        dataSource.setUrl(url);
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        return dataSource;
    }

    public static DataSource getOracleDataSourcePool() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
        String host = "127.0.0.1";
        int port = 1521;
        String sid = "orcl";
        String url = String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, sid);
        dataSource.setJdbcUrl(url);
        dataSource.setUsername("etl_base");
        dataSource.setPassword("etl_base");
//        dataSource.setMaximumPoolSize(4);
        return dataSource;
    }

    public static DataSource getDuckDBDataSource(String filePath) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        Properties connectionProperties = new Properties();
        connectionProperties.put("duckdb.read_only", "true");
        dataSource.setConnectionProperties(connectionProperties);
        dataSource.setDriverClassName("org.duckdb.DuckDBDriver");
        String url = "jdbc:duckdb:" + filePath;
        dataSource.setUrl(url);
        return dataSource;
    }

    public static DataSource getPostgresDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        String host = "127.0.0.1";
        int port = 5432;
        String sid = "postgres";
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, sid);
        dataSource.setUrl(url);
        dataSource.setUsername("postgres");
        dataSource.setPassword("123");
        return dataSource;
    }

    public static DataSource getPostgresDataSourcePool() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        String host = "127.0.0.1";
        int port = 5432;
        String sid = "postgres";
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, sid);
        dataSource.setJdbcUrl(url);
        dataSource.setUsername("postgres");
        dataSource.setPassword("123");
//        dataSource.setMaximumPoolSize(4);
        return dataSource;
    }

}
