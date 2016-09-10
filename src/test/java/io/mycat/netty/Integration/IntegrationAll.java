package io.mycat.netty.Integration;

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
@Suite.SuiteClasses({LoginTest.class, ShowTest.class, ProxyInterfaceTest.class})
public class IntegrationAll {
    private static final Logger logger = LoggerFactory.getLogger(IntegrationAll.class);

    // 启动一个线程，启动我的应用

    @BeforeClass
    public static void beforeClass4IntegrationAll(){
        logger.info("before 4 integration all");
    }

    @AfterClass
    public static void afterClass4IntegrationAll(){
        logger.info("after 4 integration all");
    }
}
