package io.mycat.netty.mysql;

import io.mycat.netty.TestUtil;
import io.mycat.netty.conf.Configuration;
import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.handler.SyncMysqlSessionContext;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.packet.OkPacket;
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


        System.out.println(" ============ ");
        System.out.println(" ============ ");
        System.out.println(" ============ ");
        System.out.println(" ============ ");
        System.out.println(" ============ ");
        System.out.println(" ============ ");
        System.out.println(" ============ ");
        System.out.println(" ============ ");
        System.out.println(" ============ ");

    }

    // d1 :
    // insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(1,1,1, '2016-01-01', '2016-01-01', 1);
    // insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(3,3,3, '2016-01-01', '2016-01-01', 1);
    // insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(5,5,5, '2016-01-01', '2016-01-01', 1);
    // d0 :
    // insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(2,2,2, '2016-01-01', '2016-01-01', 1);
    // insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(4,4,4, '2016-01-01', '2016-01-01', 1);
    @Test
    public void testSelect() throws InterruptedException {
        // single select
        String stmt;
        stmt = "select order_id, product_id, usr_id from tb0 where order_id=5";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.SELECT);


        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCurrentStatus(MysqlSessionContext.STATUS.INIT);
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("select  pakcet should be resultPacket", mySQLPacket instanceof ResultSetPacket);
            ResultSetPacket resultSetPacket = (ResultSetPacket)mySQLPacket;
            logger.info("rows len : {}", resultSetPacket.getRows().size());
            TestUtil.ROWOutput(resultSetPacket.getRows());
            Assert.assertEquals("tb0 field should be 3", 3, resultSetPacket.getFields().size());
            Assert.assertEquals("should only one data in db", 1, resultSetPacket.getRows().size());
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();


        // multi select
        stmt = "select order_id, product_id, usr_id from tb0 where order_id in (1,2,3,4,5)";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.SELECT);

        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCurrentStatus(MysqlSessionContext.STATUS.INIT);
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("select  pakcet should be resultPacket", mySQLPacket instanceof ResultSetPacket);
            ResultSetPacket resultSetPacket = (ResultSetPacket)mySQLPacket;
            logger.info("rows len : {}", resultSetPacket.getRows().size());
            TestUtil.ROWOutput(resultSetPacket.getRows());
            Assert.assertEquals("tb0 field should be 3", 3, resultSetPacket.getFields().size());
            Assert.assertEquals("should only one data in db", 13, resultSetPacket.getRows().size());
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();
        logger.info("finish ");




        // insert
        stmt = "insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(12,12,12, '2016-01-01', '2016-01-01', 1)";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.INSERT);

        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCurrentStatus(MysqlSessionContext.STATUS.INIT);
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("select  pakcet should be okPacket", mySQLPacket instanceof OkPacket);
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();
        logger.info("finish ");


        // update
        stmt = "update tb0 set status=2 where order_id=12";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.UPDATE);

        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCurrentStatus(MysqlSessionContext.STATUS.INIT);
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("update pakcet should be okPacket", mySQLPacket instanceof OkPacket);
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();
        logger.info("finish ");


        // select ensurence
        stmt = "select order_id, product_id, usr_id from tb0 where order_id=12";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.SELECT);

        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCurrentStatus(MysqlSessionContext.STATUS.INIT);
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("select  pakcet should be resultPacket", mySQLPacket instanceof ResultSetPacket);
            ResultSetPacket resultSetPacket = (ResultSetPacket)mySQLPacket;
            logger.info("rows len : {}", resultSetPacket.getRows().size());
            TestUtil.ROWOutput(resultSetPacket.getRows());
            Assert.assertEquals("tb0 field should be 3", 3, resultSetPacket.getFields().size());
            Assert.assertEquals("should only one data in db", 1, resultSetPacket.getRows().size());
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();
        logger.info("finish ");

        // delete
        stmt = "delete from tb0 where order_id=12";
        mysqlSessionContext.setSql(stmt);
        mysqlSessionContext.setType(ServerParse.DELETE);

        mysqlSessionContext.setBlocking(new CountDownLatch(1));
        mysqlSessionContext.setCurrentStatus(MysqlSessionContext.STATUS.INIT);
        mysqlSessionContext.setCheck(mySQLPacket -> {
            Assert.assertTrue("delete pakcet should be okPacket", mySQLPacket instanceof OkPacket);
        });
        // should route, send2Server, then async, receive, write2client
        mysqlSessionContext.process();

        mysqlSessionContext.blocking();
        logger.info("finish ");

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
