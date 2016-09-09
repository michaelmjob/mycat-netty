package io.mycat.netty.config;

import io.mycat.netty.conf.Configuration;
import io.mycat.netty.mysql.backend.SessionService;
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

        Map<String, DataSource> dataSourceMap = SessionService.getDataSources();

        Assert.assertEquals(2, dataSourceMap.size());

        checkConsistency(dataSourceMap.get("d0"), "db0",6);
        checkConsistency(dataSourceMap.get("d1"), "db1", 8);

        System.out.println("finish test");
    }


    private void checkConsistency(DataSource dataSource, String dbname, int size){
        for(Host host : dataSource.getAllHosts()) {
            int truesize = host.connectionSize(dbname, true);
            int falsesize = host.connectionSize(dbname, false);
            logger.info(" true size : {}", truesize);
            logger.info(" false size : {}", falsesize);
            junit.framework.Assert.assertEquals(size, truesize);
            junit.framework.Assert.assertEquals(0, falsesize);
        }
    }
}
