package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by snow_young on 16/8/14.
 */
public class MysqlDataSourceTest {
    private static final Logger logger = LoggerFactory.getLogger(MysqlDataSourceTest.class);

    private DataSourceConfig.HostConfig readConfig;
    private DataSourceConfig.HostConfig writeConfig;
    private DataSourceConfig.DatanodeConfig datanodeConfig;
    private String[] schemas;

    @Before
    public void setUp(){
//        (String name, DataSourceConfig.DatanodeConfig datanodeConfig, String[] schemas
        readConfig = new DataSourceConfig.HostConfig();
        writeConfig = new DataSourceConfig.HostConfig();
        datanodeConfig = new DataSourceConfig.DatanodeConfig();

        readConfig.setUser("root");
        readConfig.setPassword("xujianhai");
        readConfig.setUrl("localhost:3306");
        readConfig.setReadType(true);

        writeConfig.setUser("root");
        writeConfig.setPassword("xujianhai");
        writeConfig.setUrl("localhost:3306");
        writeConfig.setReadType(true);

        datanodeConfig.setBalance("balance strategy");
        datanodeConfig.setDbdriver("default");
        datanodeConfig.setDbtype("mysql");
        datanodeConfig.setMaxconn(100);
        datanodeConfig.setMinconn(10);
        // is extra
        datanodeConfig.setName("db0");
        List<DataSourceConfig.HostConfig> hostConfigList = new ArrayList<DataSourceConfig.HostConfig>();
        hostConfigList.add(readConfig);
        datanodeConfig.setReadhost(hostConfigList);
        datanodeConfig.setReadtype(true);

        datanodeConfig.setWritehost(writeConfig);

        schemas = new String[1];
        schemas[0] = "mydb";

    }


    @Test
    public void testInit(){
        DataSource dataSource = new MysqlDataSource("db0", datanodeConfig, schemas);
        dataSource.init();

        for(Host host : dataSource.getAllHosts()){
            int truesize = host.connectionSize("mydb", true);
            int falsesize = host.connectionSize("mydb", false);
            logger.info("true size : {}", truesize);
            logger.info("false size : {}", falsesize);
            Assert.assertEquals(20, truesize);
        }

    }
}
