package io.mycat.netty.mysql;

import io.mycat.netty.mysql.backend.BackendAllTest;
import io.mycat.netty.mysql.backend.HandshakeTest;
import io.mycat.netty.mysql.backend.SessionServiceTest;
import io.mycat.netty.mysql.backend.datasoource.MysqlDataSourceTest;
import io.mycat.netty.mysql.backend.datasoource.MysqlHostTest;
import io.mycat.netty.mysql.handler.HandleAllTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/9/10.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = { BackendAllTest.class, HandleAllTest.class})
public class MysqlAllTest {
        private static final Logger logger = LoggerFactory.getLogger(MysqlAllTest.class);

        @BeforeClass
        public static void beforeClass4MysqlAll(){
                logger.info("before 4 mysql all");
        }

        @AfterClass
        public static void afterClass4MysqlAll(){
                logger.info("after 4 mysql all");
        }
}
