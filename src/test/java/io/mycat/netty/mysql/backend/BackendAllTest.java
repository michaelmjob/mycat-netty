package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.MysqlDataNode;
import io.mycat.netty.config.ConfigurationTest;
import io.mycat.netty.config.schemaLoaderTest;
import io.mycat.netty.config.userLoaderTest;
import io.mycat.netty.mysql.backend.datasoource.MysqlDataSourceTest;
import io.mycat.netty.mysql.backend.datasoource.MysqlHostTest;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import io.mycat.netty.mysql.backend.datasource.MysqlHost;
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
@Suite.SuiteClasses(value = { MysqlDataSourceTest.class, MysqlHostTest.class,
                    HandshakeTest.class, SessionServiceTest.class})
public class BackendAllTest {

    public static final Logger logger = LoggerFactory.getLogger(BackendAllTest.class);

    @BeforeClass
    public static void beforeClassConfig(){
        logger.info("before class in backend all test");
    }

    @AfterClass
    public static void afterClassConfig(){

        logger.info("after class in backend all test");
    }

}
