package io.mycat.netty.Integration;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.sun.source.tree.AssertTree;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by snow_young on 16/8/5.
 */
public class LoginTest {

    @Test
    public void testLoginWithPassSucceess() {
        try {
            //加载MySql的驱动类
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
        }

        String username = "xujianhai";
        String password = "xujianhai";
        String dbUrl = "jdbc:mysql://localhost:8090/schema?connectTimeout=1000";
        // test success
        Connection con = null;
        try {
            con = DriverManager.getConnection(dbUrl, username, password);
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

    @Test
    public void testLoginWithPassWrongPass(){
        try {
            //加载MySql的驱动类
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        String username = "xujianhai";
        String wrongpass = "zhangsan";
        String dbUrl = "jdbc:mysql://localhost:8090/schema?connectTimeout=1000";
        Connection con = null;
        try {
//            thrown.expect(SQLException.class);
//            thrown.expectMessage("Access denied for user 'xujianhai'");
            con = DriverManager.getConnection(dbUrl, username, wrongpass);
            Assert.assertTrue(false);
        } catch (SQLException se) {
            System.out.println("数据库连接失败！");
            se.printStackTrace();
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

    @Test
    public void testLoginWithPassWrongDB(){
        try {
            //加载MySql的驱动类
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        String username = "xujianhai";
        String wrongpass = "zhangsan";
        String dbUrl = "jdbc:mysql://localhost:8090/schemawrong?connectTimeout=1000";
        Connection con = null;
        try {
//            thrown.expect(SQLException.class);
//            thrown.expectMessage("Access denied for user 'xujianhai'");
            con = DriverManager.getConnection(dbUrl, username, wrongpass);
            Assert.assertTrue(false);
        } catch (SQLException se) {
            System.out.println("数据库连接失败！");
            se.printStackTrace();
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

    @Test
    public void testLoginWithPassWrongUser(){
        try {
            //加载MySql的驱动类
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        String username = "xujianhaiwronguser";
        String wrongpass = "zhangsan";
        String dbUrl = "jdbc:mysql://localhost:8090/schema?connectTimeout=1000";
        Connection con = null;
        try {
            con = DriverManager.getConnection(dbUrl, username, wrongpass);
            Assert.assertTrue(false);
        } catch (SQLException se) {
            System.out.println("数据库连接失败！");
            se.printStackTrace();
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
