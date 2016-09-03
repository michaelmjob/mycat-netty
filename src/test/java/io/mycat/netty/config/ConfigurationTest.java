package io.mycat.netty.config;

import io.mycat.netty.conf.Configuration;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by snow_young on 16/8/15.
 */
public class ConfigurationTest {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationTest.class);

    @Test
    public void testLoad_datasource(){
        Configuration.init();

        Map<String, DataSource> dataSourceMap = Configuration.getDataSources();

        Assert.assertEquals(2, dataSourceMap.size());

        checkConsistency(dataSourceMap.get("d0"), "db0",30);
        checkConsistency(dataSourceMap.get("d1"), "db1", 30);

        System.out.println("finish test");
    }


    private void checkConsistency(DataSource dataSource, String dbname, int size){
        for(Host host : dataSource.getAllHosts()){
            int d0truesize = host.connectionSize("db0", true);
            int d0falsesize = host.connectionSize("db0", false);
            int d1truesize = host.connectionSize("db1", true);
            int d1falsesize = host.connectionSize("db1", false);
            int checksize = host.connectionSize(dbname, true);
            logger.info("db0 true size : {}", d0truesize);
            logger.info("db0 false size : {}", d0falsesize);
            logger.info("db1 true size : {}", d1truesize);
            logger.info("db1 false size : {}", d1falsesize);
            junit.framework.Assert.assertEquals(size, checksize);
//            junit.framework.Assert.assertEquals(size, d1truesize);
        }
    }
}
