package io.mycat.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.function.Supplier;

/**
 * Created by snow_young on 16/9/9.
 */
public class TestDBUtil {
    private static Logger logger = LoggerFactory.getLogger(TestDBUtil.class);

    public static void showDB(Supplier<Connection> getConn) throws SQLException {
        Connection conn = getConn.get();
        logger.info("show databases");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("show databases");
        while (resultSet.next()) {
            logger.info(resultSet.getString(1));
        }
        conn.close();
    }

    public static void showTB(Supplier<Connection> getConn) throws SQLException {
        Connection conn = getConn.get();
        Statement statement = conn.createStatement();
        logger.info("show tables");
        ResultSet resultSet = statement.executeQuery("show tables");
        while (resultSet.next()) {
            logger.info(resultSet.getString(1));
        }
        conn.close();
    }
}
