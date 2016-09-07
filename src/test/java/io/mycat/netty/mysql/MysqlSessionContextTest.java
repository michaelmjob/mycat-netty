package io.mycat.netty.mysql;

import io.mycat.netty.conf.Configuration;
import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.handler.SyncMysqlSessionContext;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.parser.ServerParse;
import io.mycat.netty.mysql.response.ResultSetPacket;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Created by snow_young on 16/9/6.
 */
public class MysqlSessionContextTest {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSessionContext.class);

    private static MysqlFrontendSession frontendSession;

    private static SyncMysqlSessionContext mysqlSessionContext;

    @BeforeClass
    public static void beforeClass(){
        init();

        frontendSession = Mockito.mock(MysqlFrontendSession.class);
        Mockito.when(frontendSession.isAutocommit()).thenReturn(true);
        Mockito.when(frontendSession.isClosed()).thenReturn(false);
        Mockito.when(frontendSession.getCharset()).thenReturn("utf8");
        Mockito.when(frontendSession.getSchema()).thenReturn("frontdb0");
        Mockito.doNothing().when(frontendSession).writeAndFlush(Mockito.any(MySQLPacket.class));

        mysqlSessionContext = new SyncMysqlSessionContext(frontendSession);
    }

    public static void init(){
        // is there any good ideal
        XMLSchemaLoader schemaLoader = new XMLSchemaLoader();
        schemaLoader.setSchemaFile("/SessionContext.xml");
        Configuration.setSchemaLoader(schemaLoader);
        Configuration.init();
    }

    @Test
    public void testSelect() throws InterruptedException {
        // single select
        String stmt = "select order_id, product_id, usr_id from tb0 where order_id=5";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.SELECT);


        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("select  pakcet should be resultPacket", mySQLPacket instanceof ResultSetPacket);
            ResultSetPacket resultSetPacket = (ResultSetPacket)mySQLPacket;
            logger.info("rows len : {}", resultSetPacket.getRows().size());
            Assert.assertEquals("tb0 field should be 3", 3, resultSetPacket.getFields().size());
            Assert.assertEquals("should only one data in db", 1, resultSetPacket.getRows().size());
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();


        // multi select
        stmt = "select order_id from tb0 where order_id in (1,2,3,4,5)";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.SELECT);

        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("select  pakcet should be resultPacket", mySQLPacket instanceof ResultSetPacket);
            ResultSetPacket resultSetPacket = (ResultSetPacket)mySQLPacket;
            logger.info("rows len : {}", resultSetPacket.getRows().size());
            Assert.assertEquals("tb0 field should be 3", 3, resultSetPacket.getFields().size());
            Assert.assertEquals("should only one data in db", 1, resultSetPacket.getRows().size());
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();
    }

//    @Test
    public void testInsert(){

    }

//    @Test
    public void testUpdate(){

    }


//    @Test
    public void testDelete(){

    }
}
