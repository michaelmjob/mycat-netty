package io.mycat.netty.mysql.handler;

import io.mycat.netty.mysql.backend.handler.MultiNodeHandler;
import io.mycat.netty.mysql.backend.handler.SingleNodeHandler;
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
@Suite.SuiteClasses({MultiNodeHandlerTest.class, SingleNodeHandlerTest.class})
public class HandleAllTest {
    private static final Logger logger = LoggerFactory.getLogger(HandleAllTest.class);

    @BeforeClass
    public static void beforeClass4HandleAll(){
        logger.info("before 4 handle all");
    }

    @AfterClass
    public static void afterClass4HandleAll(){
        logger.info("after 4 handle all");
    }


}
