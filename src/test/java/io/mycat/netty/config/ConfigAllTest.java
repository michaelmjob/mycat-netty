package io.mycat.netty.config;

import io.mycat.netty.conf.Configuration;
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
@Suite.SuiteClasses(value = {ConfigurationTest.class, schemaLoaderTest.class, userLoaderTest.class})
public class ConfigAllTest {
    public static final Logger logger = LoggerFactory.getLogger(ConfigAllTest.class);

    @BeforeClass
    public static void beforeClassConfig(){
        logger.info("before class in config all test");
    }

    @AfterClass
    public static void afterClassConfig(){

        logger.info("after class in config all test");
    }
}
