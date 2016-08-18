package io.mycat.netty.mysql;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by snow_young on 16/8/9.
 *
 * 需要添加的测试
 *      processpor中添加的测试
 *
 * SET autocommit=1
 * SET character_set_results = NULL
 * SELECT @@session.auto_increment_increment
 * SHOW VARIABLES WHERE Variable_name ='language' OR Variable_name = 'net_write_timeout' OR Variable_name = 'interactive_timeout' OR Variable_name = 'wait_timeout' OR Variable_name = 'character_set_client' OR Variable_name = 'character_set_connection' OR Variable_name = 'character_set' OR Variable_name = 'character_set_server' OR Variable_name = 'tx_isolation' OR Variable_name = 'transaction_isolation' OR Variable_name = 'character_set_results' OR Variable_name = 'timezone' OR Variable_name = 'time_zone' OR Variable_name = 'system_time_zone' OR Variable_name = 'lower_case_table_names' OR Variable_name = 'max_allowed_packet' OR Variable_name = 'net_buffer_length' OR Variable_name = 'sql_mode' OR Variable_name = 'query_cache_type' OR Variable_name = 'query_cache_size' OR Variable_name = 'init_connect'
 */
public class ShowTest {
    // show variables
    // show tables
    // show databases

    // select @@version
    // select @@version_comment limit 1
    private String username = "xujianhai";
    private String password = "xujianhai";
//    private String dbUrl = "jdbc:mysql://localhost:8090/schema?connectTimeout=1000";
    private String dbUrl = "jdbc:mysql://localhost:8090/frontdb0?connectTimeout=1000";
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

    @Test
    public void testShowVariables(){
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
            ResultSet result = stmt.executeQuery("show variables");

            while(result.next()){
                Assert.assertEquals("should equal", variables.get(result.getString(1)), result.getString(2));
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

    // show tables;
    //  SHOW TABLES FROM  mydb;
//    show tables like 'my_%';
//    show columns from mytable;
//    describe mytable;
//    show create table mytable;
//    SHOW CHARACTER SET WHERE `Default collation` LIKE '%japanese%';
//     SHOW CHARACTER SET WHERE Maxlen > 1;
//    @Test
//    public void testShowTables(){
//
//    }

    @Test
    public void testShowFullTables(){
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
            ResultSet result = stmt.executeQuery("SHOW TABLES");

            System.out.println("show tables");
            while(result.next()){
                System.out.println(result.getString(1));
//                Assert.assertEquals("should equal", variables.get(result.getString(1)), result.getString(2));
            }

            System.out.println("show tables from frontdb0");
            stmt = conn.createStatement();
            result = stmt.executeQuery("SHOW TABLES FROM frontdb0");
            while(result.next()){
                System.out.println(result.getString(1));
//                Assert.assertEquals("should equal", variables.get(result.getString(1)), result.getString(2));
            }


            System.out.println("show tables from frontdb0 like 'tb%'");
            stmt = conn.createStatement();
            result = stmt.executeQuery("show tables from frontdb0 like 'tb%'");
            while(result.next()){
                System.out.println(result.getString(1));
//                Assert.assertEquals("should equal", variables.get(result.getString(1)), result.getString(2));
            }

            System.out.println("show tables like 'tb%'");
            result = stmt.executeQuery("show tables like 'tb%'");
            while(result.next()){
                System.out.println(result.getString(1));
//                Assert.assertEquals("should equal", variables.get(result.getString(1)), result.getString(2));
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
