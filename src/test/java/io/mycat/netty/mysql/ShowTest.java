package io.mycat.netty.mysql;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by snow_young on 16/8/9.
 */
public class ShowTest {
    // show variables
    // show tables
    // show databases

    // select @@version
    // select @@version_comment limit 1
    private String username = "xujianhai";
    private String password = "xujianhai";
    private String dbUrl = "jdbc:mysql://localhost:8090/schema?connectTimeout=1000";
    private final Map<String, String> variables = new HashMap<String, String>();

    private String mysqluser = "root";
    private String mysqlpass = "xujianhai";
    private String mysqlUrl = "jdbc:mysql://localhost:8090/schema?connectTimeout=1000";

    @Before
    public void setUp(){
        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "8192");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("lower_case_table_names", "1");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
    }

//    @Test
//    public void testShowVariables(){
//        try {
//            Class.forName("com.mysql.jdbc.Driver");
//        } catch (ClassNotFoundException e) {
//            System.out.println("找不到驱动程序类 ，加载驱动失败！");
//            e.printStackTrace();
//            Assert.assertTrue(false);
//        }
//
//        Connection conn = null;
//        try {
//            conn = DriverManager.getConnection(dbUrl, username, password);
//            Statement stmt = conn.createStatement();
//            ResultSet result = stmt.executeQuery("show variables");
//
//            while(result.next()){
//                Assert.assertEquals("should equal", variables.get(result.getString(1)), result.getString(2));
//            }
//
//        } catch (SQLException se) {
//            System.out.println("数据库连接失败！");
//            se.printStackTrace();
//            Assert.assertTrue(false);
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                    Assert.assertTrue(false);
//                }
//            }
//        }
//    }

    @Test
    public void testShowDatabases(){
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
            ResultSet result = stmt.executeQuery("SHOW DATABASES");

            System.out.println("show databases");
            while(result.next()){
                System.out.println(result.getString(1));
//                Assert.assertEquals("should equal", variables.get(result.getString(1)), result.getString(2));
            }

        } catch (SQLException se) {
            System.out.println("数据库连接失败！");
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

//    @Test
//    public void testShowTables(){
//
//    }

}
