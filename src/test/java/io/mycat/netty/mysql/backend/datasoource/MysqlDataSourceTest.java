package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.mysql.TestConstants;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
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

    private static String[] schemas;

    private static DataSource dataSource;

    @BeforeClass
    public static void beforeMysqlDatasOurce(){

    }

    // prepare config
    @Before
    public void setUp(){

        super.init();

        schemas = new String[1];
        schemas[0] = TestConstants.DB0;
    }

    @After
    public void teardown(){

    }


    @Test
    public void testInit(){
        dataSource = new MysqlDataSource(TestConstants.D0, datanodeConfig, schemas);
        dataSource.init();

        for(Host host : dataSource.getAllHosts()){
            int truesize = host.connectionSize(TestConstants.DB0, true);
            int falsesize = host.connectionSize(TestConstants.DB0, false);
            logger.info("true size : {}", truesize);
            logger.info("false size : {}", falsesize);
            Assert.assertEquals(10, truesize);
        }
    }

    @Test
    public void testReadBalance(){
        dataSource = new MysqlDataSource(TestConstants.D0, datanodeConfig, schemas);
        dataSource.init();

//        dataSource.
    }


    @Test
    public void testWrongInit(){

    }

}
