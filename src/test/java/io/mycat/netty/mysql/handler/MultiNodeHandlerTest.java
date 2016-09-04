package io.mycat.netty.mysql.handler;

import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.BackendTest;
import io.mycat.netty.mysql.backend.SessionService;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Created by snow_young on 16/8/21.
 */
public class MultiNodeHandlerTest extends BackendTest {
    private static Logger logger = LoggerFactory.getLogger(MultiNodeHandlerTest.class);

    private static MysqlFrontendSession frontendSession;

    @BeforeClass
    public static void beforeClass() throws IOException, SAXException, ParserConfigurationException {
        init();

        frontendSession = Mockito.mock(MysqlFrontendSession.class);
        Mockito.when(frontendSession.isAutocommit()).thenReturn(true);
        Mockito.when(frontendSession.isClosed()).thenReturn(false);
        Mockito.when(frontendSession.getCharset()).thenReturn("utf8");
        Mockito.when(frontendSession.getConnectionId()).thenReturn(Long.valueOf(1));
        Mockito.doNothing().when(frontendSession).writeAndFlush(Mockito.any(MySQLPacket.class));
    }

    // test multi write
    //  not care of two host write fail scenary
    @Test
    public void testWrite() throws InterruptedException {
        String dataNodeName1 = "d1";
        String databaseName1 = "db1";
        String dataNodeName2 = "d0";
        String databaseName2 = "db0";
        String insert = "insert into tb0 values(3,1,1,'2016-01-01', '2016-01-01', 1)";
        String insert2 = "insert into tb0 values(4,1,1,'2016-01-01', '2016-01-01', 1)";
        // just for test, not used in production
        String select = "select * from tb0";
        String update = "update tb0 set status=2";
        String delete = "delete from tb0";

        // build route, ensuere host exists after route
        RouteResultsetNode node_insert1 = new RouteResultsetNode(dataNodeName1, databaseName1, insert);
        RouteResultsetNode node_insert2 = new RouteResultsetNode(dataNodeName2, databaseName2, insert);
        RouteResultsetNode[] nodeArr = new RouteResultsetNode[]{node_insert1, node_insert2};
//        RouteResultsetNode[] nodeArr = new RouteResultsetNode[]{node_insert1};
        RouteResultset routeResultset = new RouteResultset();
        routeResultset.setNodes(nodeArr);

        //  test write success, okpacket group
//        testSQL(routeResultset, mySQLPacket -> {
//            Assert.assertTrue("insert success pakcet should be okPacket", mySQLPacket instanceof OkPacket);
//        });

        // test write error, error group
        // build route, ensuere host exists after route
        String error_insert = "insert into tb0 values(3,1,1,'2016-01-01', '2016-01-01')";
        node_insert1 = new RouteResultsetNode(dataNodeName1, databaseName1, error_insert);
        node_insert2 = new RouteResultsetNode(dataNodeName2, databaseName2, error_insert);
        nodeArr = new RouteResultsetNode[]{node_insert1, node_insert2};
        routeResultset = new RouteResultset();
        routeResultset.setNodes(nodeArr);

        // error group
        testSQL(routeResultset, mySQLPacket -> {
            Assert.assertTrue("insert success pakcet should be okPacket", mySQLPacket instanceof ErrorPacket);
            ErrorPacket errorPacket = (ErrorPacket)mySQLPacket;
            logger.info("error Packet errno : {}", errorPacket.errno);
//            logger.info("error Packet err msg : {}", errorPacket.message.toString().getBytes());
            logger.info("error Packet err msg : {}", new String(errorPacket.message));
        });
    }

    public void testSQL(RouteResultset routeResultset, Consumer<MySQLPacket> check) throws InterruptedException {
        SyncMysqlSessionContext blockingMysqlSessionContext = new SyncMysqlSessionContext(frontendSession);

        blockingMysqlSessionContext.setRrs(routeResultset);
        blockingMysqlSessionContext.getSession();

        blockingMysqlSessionContext.setBlocking(new CountDownLatch(1));
        blockingMysqlSessionContext.send();


        blockingMysqlSessionContext.setCheck(check);
        blockingMysqlSessionContext.blocking();
        logger.info("sql send && receive finish");
    }


    public void testDelete() {

    }

    public void testUpdate() {

    }

    public void testSelect() {

    }


}
