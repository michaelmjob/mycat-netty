package org.mycat.netty.mysql;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by snow_young on 16/8/5.
 */
public class protocolTest {
    private String dbUrl = "jdbc:mysql://localhost:8090/schema_main?connectTimeout=1000";
//    private String dbUrl = "jdbc:mysql://localhost:6100/schema_main?connectTimeout=1000";

    @Test
    public void loginWithoutPass() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        try {
            //加载MySql的驱动类
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
        }

        String username = "root";
        String password = "root";
        // test success
        Connection con = null;
        try {
            con = DriverManager.getConnection(dbUrl, username, password);
        } catch (SQLException se) {
            System.out.println("数据库连接失败！");
            se.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
