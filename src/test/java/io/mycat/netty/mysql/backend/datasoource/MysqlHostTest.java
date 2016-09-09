package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.mysql.Constants;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlHost;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by snow_young on 16/8/14.
 *
 * more test should be added
 */
public class MysqlHostTest extends CommonTest{
    private static final Logger logger = LoggerFactory.getLogger(MysqlHostTest.class);


    @BeforeClass
    public static void beforeMysqlDataHost(){

    }

    @Before
    public void setUp(){
        super.init();
    }

    @Test
    public void testGetConn(){
        // mysqlhostTest : arbitary name, do not care
        Host host = new MysqlHost("mysqlhostTest", hostConfig, datanodeConfig, true);

        try {
            host.init(Constants.DB0);
        } catch (InterruptedException e) {
            logger.error("init error for host", e);
            Assert.assertTrue(false);
        }

        // should ensure all connection is finished
        int size = host.connectionSize(Constants.DB0, true);

        logger.info("host size  : {}", size);
        Assert.assertEquals(10, size);

    }
}
