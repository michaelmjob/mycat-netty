package io.mycat.netty.Integration;

import io.mycat.netty.TestConstants;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.Objects;

/**
 * Created by snow_young on 16/9/12.
 */
public class ManCommitTest {

    @Test
    public void testMan(){
        try {
            //加载MySql的驱动类
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
        }

        String username = TestConstants.user;
        String password = TestConstants.pass;
        String dbUrl = "jdbc:mysql://localhost:8090/schema?connectTimeout=1000";
        // test success
        Connection con = null;
        Connection con2 = null;
        Statement select;
        Statement insert;
        ResultSet resultSet;
        try {
            con = DriverManager.getConnection(dbUrl, username, password);
            con2 = DriverManager.getConnection(dbUrl, username, password);

            con.setAutoCommit(false);

            insert = con.createStatement();
            insert.executeUpdate("insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(12,12,12, '2016-01-01', '2016-01-01', 1)");
//            PreparedStatement ptmt =  (PreparedStatement) conn.prepareStatement(sql);
//            ptmt.setString(1, man.getName());
//            ptmt.setInt(2, man.getAge());
//            ptmt.execute();

            select = con2.createStatement();
            resultSet = select.executeQuery("select order_id, product_id, usr_id from tb0 where order_id=20");
            assert !resultSet.next();

            con.commit();

            select = con2.createStatement();
            resultSet = select.executeQuery("select order_id, product_id, usr_id from tb0 where order_id=20");
            assert resultSet.next();


        } catch (SQLException se) {
            System.out.println("数据库连接失败！不应该发生");
            se.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    Assert.assertTrue(false);
                }
            }
        }
    }
}
