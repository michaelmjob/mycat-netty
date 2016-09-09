package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.mysql.Constants;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by snow_young on 16/8/14.
 *
 * check datasource init,
 *  expect host all in datasource bootstrap successfully;
 */
public class MysqlDataSourceTest extends CommonTest{
    private static final Logger logger = LoggerFactory.getLogger(MysqlDataSourceTest.class);

    private String[] schemas;

    // prepare config
    @Before
    public void setUp(){

        super.init();

        schemas = new String[1];
        schemas[0] = Constants.DB0;

    }

    @Test
    public void testInit(){
        DataSource dataSource = new MysqlDataSource(Constants.D0, datanodeConfig, schemas);
        dataSource.init();

        for(Host host : dataSource.getAllHosts()){
            int truesize = host.connectionSize(Constants.DB0, true);
            int falsesize = host.connectionSize(Constants.DB0, false);
            logger.info("true size : {}", truesize);
            logger.info("false size : {}", falsesize);
            Assert.assertEquals(20, truesize);
        }
    }

    @Test
    public void testWrongInit(){

    }

}
