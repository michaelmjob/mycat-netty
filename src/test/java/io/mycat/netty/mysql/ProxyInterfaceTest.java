package io.mycat.netty.mysql;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by snow_young on 16/9/8.
 *
 * 有时候数据验证. 数据库会失败,
 */
public class ProxyInterfaceTest {
    private static Logger logger = LoggerFactory.getLogger(ProxyInterfaceTest.class);

    private String username = "xujianhai";
    private String password = "xujianhai";
    private String dbUrl = "jdbc:mysql://localhost:8090/frontdb0?connectTimeout=1000";

    // need mysql mock
    @Test
    public void testSingleSQL() {
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
            Statement stmt;
            ResultSet result;

            // select from one datanode
            stmt = conn.createStatement();
            result = stmt.executeQuery("select order_id, product_id, usr_id from tb0 where order_id=5");
            while (result.next()) {
                logger.info( "result : order_di -> {},  product_id -> {},  user_id -> {}", result.getString(1), result.getString(2), result.getString(3));
            }


            // select from multi datanodes
            stmt = conn.createStatement();
            result = stmt.executeQuery("select order_id, product_id, usr_id from tb0 where order_id in (1,2,3,4,5)");
            while (result.next()) {
                logger.info( "result : order_di -> {},  product_id -> {},  user_id -> {}", result.getString(1), result.getString(2), result.getString(3));
            }


//            TODO: be caution
//            String query = "insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(12,12,12, '2016-01-01', '2016-01-01', 1)";
//            // create the mysql insert preparedstatement
//            PreparedStatement preparedStmt = conn.prepareStatement(query);
//            // execute the preparedstatement
//            preparedStmt.execute();


            // insert db0
            logger.info("insert db0");
            stmt = conn.createStatement();
            stmt.executeUpdate("insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(12,12,12, '2016-01-01', '2016-01-01', 1)");


            // insert db1
            logger.info("insert db1");
            stmt = conn.createStatement();
            stmt.executeUpdate("insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(13,13,13, '2016-01-01', '2016-01-01', 1)");


            // update db0
            logger.info("update db0");
            stmt = conn.createStatement();
            stmt.executeUpdate("update tb0 set status=2 where order_id=12");

            // update db1
            logger.info("update db1");
            stmt = conn.createStatement();
            stmt.executeUpdate("update tb0 set status=2 where order_id=13");

            // delete db0
            logger.info("delete db0");
            stmt = conn.createStatement();
            stmt.executeUpdate("delete from tb0 where order_id=12");


            // delete db1
            logger.info("delete db1");
            stmt = conn.createStatement();
            stmt.executeUpdate("delete from tb0 where order_id=13");

            // test error sql : in mysql
            try {
                logger.info("test error sql in mysql ");
                stmt = conn.createStatement();
                stmt.executeUpdate("delete from tb0 where orderid=13");
                Assert.assertTrue("should occur error in mysql", false);
            }catch(SQLException e){
                logger.info("should occur error in mysql");
                logger.info("detail error: {}", e);
            }

            // test error no table
            try {
                logger.info("test error sql in mysql ");
                stmt = conn.createStatement();
                stmt.executeUpdate("delete from tb20 where order_id=13");
                Assert.assertTrue("should occur error in proxy for no tb20 exist", false);
            }catch(SQLException e){
                logger.info("should occur error in proxy for no tb20 exist");
                logger.info("detail error: {}", e);
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
