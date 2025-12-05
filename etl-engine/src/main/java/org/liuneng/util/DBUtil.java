package org.liuneng.util;

import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.util.LinkedCaseInsensitiveMap;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Map;

public class DBUtil {

    public static String[] lookupColumns(DataSource ds, String tableName) throws SQLException {
        String[] columns = null;
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ds.getConnection();
            statement = connection.prepareStatement("select * from " + tableName);
            columns = new String[statement.getMetaData().getColumnCount()];
            for (int i = 0; i < statement.getMetaData().getColumnCount(); i++) {
                String targetColumn = statement.getMetaData().getColumnLabel(i + 1);
                columns[i] = targetColumn;
            }

        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return columns;
    }

    public static Map<String, Object> mapRow(ResultSet rs) throws SQLException {

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Map<String, Object> mapOfColumnValues = new LinkedCaseInsensitiveMap<>(columnCount);

        for(int i = 1; i <= columnCount; ++i) {
            String column = JdbcUtils.lookupColumnName(rsmd, i);
            mapOfColumnValues.putIfAbsent(column, JdbcUtils.getResultSetValue(rs, i));
        }

        return mapOfColumnValues;
    }

    public static int testReference() {
        return 9527;
    }
}
