package io.mycat.netty.mysql;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by snow_young on 16/9/8.
 */
public class Select2Mysql {
    private static Logger logger = LoggerFactory.getLogger(Select2Mysql.class);

    private String username = "xujianhai";
    private String password = "xujianhai";
    private String dbUrl = "jdbc:mysql://localhost:8090/frontdb0?connectTimeout=1000";

    // need mysql mock
    @Test
    public void testSQL() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(dbUrl, username, password);
            Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery("select order_id, product_id, usr_id from tb0 where order_id=5");

            while (result.next()) {
                logger.info( "result : order_di -> {},  product_id -> {},  user_id -> {}", result.getString(1), result.getString(2), result.getString(3));
            }

        } catch (SQLException se) {
            System.out.println("数据库操作失败！");
            se.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    Assert.assertTrue(false);
                }
            }
        }
    }




}
