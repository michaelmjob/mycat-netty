package io.mycat.netty.mysql.handler;

import io.mycat.netty.util.TestUtil;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.backend.BackendTest;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.mysql.response.ResultSetPacket;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by snow_young on 16/8/21.
 * <p>
 * need mock a mysql
 * test mysqlSessionContext backend action && singleNodeHandler
 */
public class SingleNodeHandlerTest extends BackendTest {
    private static Logger logger = LoggerFactory.getLogger(SingleNodeHandlerTest.class);

    private static MysqlFrontendSession frontendSession;

    @BeforeClass
    public static void beforeClass() throws IOException, SAXException, ParserConfigurationException {
        init();

        frontendSession = Mockito.mock(MysqlFrontendSession.class);
        Mockito.when(frontendSession.isAutocommit()).thenReturn(true);
        Mockito.when(frontendSession.getConnectionId()).thenReturn(Long.valueOf(1));
        Mockito.doNothing().when(frontendSession).writeAndFlush(Mockito.any(MySQLPacket.class));
    }

    /**
     * Insert:
     * insert twice, success firstly, fail secondly due to 重复的 insert
     */

    @Test
    public void testINormal() throws InterruptedException {

        String dataNodeName = "d1";
        String databaseName = "db1";
        String insert = "insert into tb0 values(3, 3 ,3, '2016-01-01', '2016-01-01', 1)";
        // just for test, not used in production
        String select = "select * from tb0";
        String update = "update tb0 set status=2";
        String delete = "delete from tb0";

        // build route, ensuere host exists after route
        RouteResultset routeResultset = TestUtil.buildSingleRouteResultSet(dataNodeName, databaseName, insert);

        // whether should live in a cycle
        // should have a status change circle

        // insert success
        TestUtil.testSQL(frontendSession, routeResultset, mySQLPacket -> {
            Assert.assertTrue("insert success pakcet should be okPacket", mySQLPacket instanceof OkPacket);
        });
        logger.info("first one : send && receive finish");

        // insert fail
        TestUtil.testSQL(frontendSession, routeResultset, mySQLPacket -> {
            Assert.assertTrue("insert distinct pakcet should be errPacket", mySQLPacket instanceof ErrorPacket);
        });
        logger.info("second : send && receive finish");

        // select
        routeResultset = TestUtil.buildSingleRouteResultSet(dataNodeName, databaseName, select);
        logger.info("select sql : {}", routeResultset.getNodes().get(0).getSql());
        TestUtil.testSQL(frontendSession, routeResultset, packet -> {
            Assert.assertTrue("select pakcet should be ResultsetPacket", packet instanceof ResultSetPacket);
            ResultSetPacket resultSetPacket = (ResultSetPacket) packet;
            Assert.assertEquals("tb0 field should be 6", 6, resultSetPacket.getFields().size());
            Assert.assertEquals("should only one data in db", 1, resultSetPacket.getRows().size());
            logger.info("insert check by select is right");
        });

//        // update
        routeResultset = TestUtil.buildSingleRouteResultSet(dataNodeName, databaseName, update);
        TestUtil.testSQL(frontendSession, routeResultset, mySQLPacket -> {
            Assert.assertTrue("update pakcet should return okPacket", mySQLPacket instanceof OkPacket);
            logger.info("update check right");
        });

        // delete
        routeResultset = TestUtil.buildSingleRouteResultSet(dataNodeName, databaseName, delete);
        TestUtil.testSQL(frontendSession, routeResultset, mySQLPacket -> {
            Assert.assertTrue("delete pakcet should return okPacket", mySQLPacket instanceof OkPacket);
            logger.info("delete check right");
        });

        // select again
        routeResultset = TestUtil.buildSingleRouteResultSet(dataNodeName, databaseName, select);
        TestUtil.testSQL(frontendSession, routeResultset, packet -> {
            Assert.assertTrue("pakcet should be ResultsetPacket", packet instanceof ResultSetPacket);
            ResultSetPacket resultSetPacket = (ResultSetPacket) packet;
            Assert.assertEquals("tb0 field should be 6", 6, resultSetPacket.getFields().size());
            Assert.assertEquals("should only one data in db", 0, resultSetPacket.getRows().size());
            logger.info("select again check right");
        });
    }

//    // 榨干session, 使重建session
//    public void testConn(){
//        // according to logic, just using the bloking send2Cient method to implement the conn accquired scene.
//
//    }
}
