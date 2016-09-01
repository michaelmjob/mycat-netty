package io.mycat.netty.mysql.handler;

import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.BackendSession;
import io.mycat.netty.mysql.backend.BackendTest;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by snow_young on 16/8/21.
 *
 * test mysqlSessionContext backend action && singleNodeHandler
 */
public class SingleNodeHandlerTest extends BackendTest {
    private static Logger logger = LoggerFactory.getLogger(SingleNodeHandlerTest.class);

    private static MysqlSessionContext mysqlSessionContext;

    private static MysqlFrontendSession frontendSession;

    @BeforeClass
    public static void beforeClass() throws IOException, SAXException, ParserConfigurationException {
        init();

        // mockito then
//        MysqlFrontendSession frontendSession = new MysqlFrontendSession();
//        mysqlSessionContext = new MysqlSessionContext(frontendSession);

//        frontendSession.setSchema("db0");
//        frontendSession.setAutocommit(true);
        frontendSession = Mockito.mock(MysqlFrontendSession.class);
        Mockito.when(frontendSession.isAutocommit()).thenReturn(true);
        Mockito.doNothing().when(frontendSession).writeAndFlush(Mockito.any(MySQLPacket.class));

        mysqlSessionContext = new MysqlSessionContext(frontendSession);
//        mysqlSessionContext.setFrontSession(frontendSession);

//        ResponseHandler responseHandler = Mockito.spy(ResponseHandler.class);
//        Mockito.doNothing().when(responseHandler).okResponse(Mockito.any(OkPacket.class), Mockito.any(NettyBackendSession.class));

    }



    @Test
    public void testInsert(){

        // 这里隐藏着bug, 并没有检查数据库名字，而是直接发送给了 datanodeName 所在的节点
        String dataNodeName= "d1";
        String databaseName = "db4";
        String insert = "insert into tb0 values(3,1,1,'2016-01-01', '2016-01-01', 1)";

        // build route, ensuere host exists after route
        RouteResultsetNode node = new RouteResultsetNode(dataNodeName, databaseName, insert);
        RouteResultsetNode[] nodeArr = new RouteResultsetNode[]{node};
        RouteResultset routeResultset = new RouteResultset();
        routeResultset.setNodes(nodeArr);

        // whether should live in a cycle
//        Host host = sessionService.getSession("d0", true);
//        node.setHost(host);

        // should have a status change circle
        mysqlSessionContext.setRrs(routeResultset);
        mysqlSessionContext.getSession();

        mysqlSessionContext.send();
//        mysqlSessionContext


//        coun
    }

    @Test
    public void testDelete(){

    }

    @Test
    public void testUpdate(){

    }

    @Test
    public void testSelect(){

    }

}
