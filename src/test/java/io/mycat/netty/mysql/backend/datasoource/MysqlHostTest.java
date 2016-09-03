package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlHost;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by snow_young on 16/8/14.
 */
public class MysqlHostTest {
    private static final Logger logger = LoggerFactory.getLogger(MysqlHostTest.class);

    private DataSourceConfig.HostConfig hostConfig;
    private DataSourceConfig.HostConfig writeConfig;
    private DataSourceConfig.DatanodeConfig datanodeConfig;

    @Before
    public void setUp(){
        hostConfig = new DataSourceConfig.HostConfig();
        writeConfig = new DataSourceConfig.HostConfig();
        datanodeConfig = new DataSourceConfig.DatanodeConfig();

        hostConfig.setUser("root");
        hostConfig.setPassword("xujianhai");
        hostConfig.setUrl("localhost:3306");
        hostConfig.setReadType(true);

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
        hostConfigList.add(hostConfig);
        datanodeConfig.setReadhost(hostConfigList);
        datanodeConfig.setReadtype(true);

        datanodeConfig.setWritehost(writeConfig);
    }

    @Test
    public void testGetConn(){
//        Host host = new MysqlHost(hostConfig, datanodeConfig, true, "mydb");
        Host host = new MysqlHost("mysqlhostTest", hostConfig, datanodeConfig, true);

        try {
            host.init("mydb");
        } catch (InterruptedException e) {
            logger.error("init error for host", e);
            Assert.assertTrue(false);
        }

        // should ensure all connection is finished
        int size = host.connectionSize("mydb", true);

        logger.info("host size  : {}", size);
        Assert.assertEquals(10, size);

//        ResponseHandler handler = new ResponseHandler() {
//            @Override
//            public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
//                Assert.assertTrue(false);
//                logger.error("error response : {}", packet);
//            }
//
//            @Override
//            public void okResponse(OkPacket packet, NettyBackendSession session) {
//                logger.error("ok response : {}", packet);
//            }
//
//            @Override
//            public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
//                logger.info("resultsetPacket : {}", resultSetPacket);
//            }
//        };


//        try {
//            host.getConnection("mydb", true, handler);
//        } catch (IOException e) {
//            Assert.assertFalse(true);
//
//        }



//  public void getConnection(String schema, boolean autocommit,
//        final ResponseHandler handler)

    }






}
