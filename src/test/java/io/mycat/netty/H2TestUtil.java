package io.mycat.netty;

import com.alibaba.druid.support.spring.stat.annotation.Stat;
import io.mycat.netty.conf.SystemConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by snow_young on 16/9/7.
 */
public class H2TestUtil {
    private static final Logger logger = LoggerFactory.getLogger(H2TestUtil.class);

    public static Server server = null;
    public static final String path = System.getProperty("user.dir") + "/src/test/resources/h2db";
    public static final String resourcesDir = System.getProperty("user.dir") + "/src/test/resources";
    public static final String userName  = "root";
    public static final String pass = "xujianhai";


    public static final String CREATETB0 = " create table tb0(" +
            " order_id INT NOT NULL," +
            " product_id INT NOT NULL," +
            " usr_id INT NOT NULL," +
            " begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
            " end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
            " status INT," +
            " PRIMARY KEY(order_id)" +
            " )";
    public static final String CREATETB1 = " create table tb1(" +
            " order_id INT NOT NULL," +
            " product_id INT NOT NULL," +
            " usr_id INT NOT NULL," +
            " begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
            " end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
            " status INT," +
            " PRIMARY KEY(order_id)" +
            " )";


    public static void beforeClass() throws SQLException {

        File dir = new File(resourcesDir);
        for(File file : dir.listFiles()){
            if(file.getName().startsWith("h2db")){
                file.delete();
            }
        }

        String[] args = new String[4];
        args[0] = "-tcpPort";
        args[1] = "3306";
        args[2] = "-tcpPassword";
        args[3] = "xujianhai";
        server = Server.createTcpServer(args).start();
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        logger.info("main here");

        beforeClass();

        logger.info("path : {}", path);

        Class.forName("org.h2.Driver");

        createDBWithTB("db0");
        createDBWithTB("db1");

        showDB();
        showTB("DB0");
        showTB("DB1");
        showDB();

        afterClass();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }

    public static void showDB() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:" + path + ":db0", userName, pass);
        logger.info("show databases");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("show databases");
        while (resultSet.next()) {
            logger.info(resultSet.getString(1));
        }
        conn.close();
    }


    public static void showTB(String db) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:" + path + ":" + db, userName, pass);
        Statement statement = conn.createStatement();
        logger.info("show tables");
        ResultSet resultSet = statement.executeQuery("show tables");
        while (resultSet.next()) {
            logger.info(resultSet.getString(1));
        }
        conn.close();
    }

    public static void createDBWithTB(String db) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:" + path + ":" + db, userName, pass);

        boolean flag;
        Statement statement;

        // tb0
        statement = conn.createStatement();
        flag = statement.execute(CREATETB0);
        assert flag;

        // tb1
        statement = conn.createStatement();
        flag = statement.execute(CREATETB1);
        assert flag;

        conn.close();
    }


    public static void afterClass() {
        logger.info("stop!");
        server.stop();
    }

}
