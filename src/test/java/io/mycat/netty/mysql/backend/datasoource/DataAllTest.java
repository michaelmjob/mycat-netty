package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.mysql.backend.HandshakeTest;
import io.mycat.netty.mysql.backend.SessionServiceTest;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import io.mycat.netty.mysql.backend.datasource.MysqlHost;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/9/9.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = { MysqlDataSourceTest.class, MysqlHostTest.class})
public class DataAllTest {
    private  static final Logger logger = LoggerFactory.getLogger(DataAllTest.class);

    @BeforeClass
    public static void beforeClass4DataAll(){
        logger.info("before class");
    }


    @AfterClass
    public static void afterClass4DataAll(){
        logger.info("after class");
    }
}
