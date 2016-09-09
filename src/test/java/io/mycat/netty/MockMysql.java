package io.mycat.netty;

import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.EmbeddedMysql;
import junit.framework.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.ScriptResolver.classPathScript;
import static com.wix.mysql.ScriptResolver.classPathScripts;
import static com.wix.mysql.distribution.Version.v5_6_23;
import static com.wix.mysql.config.Charset.UTF8;
import static java.sql.DriverManager.*;

/**
 * Created by snow_young on 16/9/8.
 * 太依赖平台了，有什么好方法进行测试？
 * 使用docker 进行测试，还是很好的，可以很好的隔离测试环境，但是时间就太长了
 */
public class MockMysql {
    private static final Logger logger = LoggerFactory.getLogger(MockMysql.class);

    public static final String userName  = "xujianhai";
    public static final String pass = "xujianhai";
    public static final String db0url = "jdbc:mysql://localhost:3306/db0";
    public static final String db1url = "jdbc:mysql://localhost:3306/db1";

    public static void main(String[] args) throws SQLException {
        MysqldConfig config = aMysqldConfig(v5_6_23)
                .withCharset(UTF8)
                .withPort(3306)
                .withUser("xujianhai", "xujianhai")
                .withTimeZone("Europe/Vilnius")
                .build();

        EmbeddedMysql mysqld = anEmbeddedMysql(config)
                .addSchema("db0", classPathScript("/db0.sql"))
                .addSchema("db1", classPathScripts("/db1.sql"))
                .start();


        //do work
        TestDBUtil.showDB(() ->{
            try {
                return getConnection(db0url, userName, pass);
            } catch (SQLException e) {
                logger.info("get conn fail, error : {}", e);
                Assert.assertTrue(false);
                return null;
            }
        });

        TestDBUtil.showTB(() -> {
            try {
                return getConnection(db0url, userName, pass);
            } catch (SQLException e) {
                logger.info("get conn fail, error : {}", e);
                Assert.assertTrue(false);
                return null;
            }
        });

        TestDBUtil.showTB(() -> {
            try {
                return getConnection(db1url, userName, pass);
            } catch (SQLException e) {
                logger.info("get conn fail, error : {}", e);
                Assert.assertTrue(false);
                return null;
            }
        });


            mysqld.stop(); //optional, as there is a shutdown hook
        }
    }
