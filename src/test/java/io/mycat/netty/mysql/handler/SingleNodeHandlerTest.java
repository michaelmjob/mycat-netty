package io.mycat.netty.mysql.handler;

import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.BackendSession;
import io.mycat.netty.mysql.backend.BackendTest;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
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

    @BeforeClass
    public static void beforeClass() throws IOException, SAXException, ParserConfigurationException {
        init();

        // mockito then
//        MysqlFrontendSession frontendSession = new MysqlFrontendSession();
//        mysqlSessionContext = new MysqlSessionContext(frontendSession);

//        frontendSession.setSchema("db0");
//        frontendSession.setAutocommit(true);
        MysqlFrontendSession frontendSession = Mockito.spy(MysqlFrontendSession.class);
        Mockito.when(frontendSession.getSchema()).thenReturn("db0");
        Mockito.when(frontendSession.isAutocommit()).thenReturn(true);
        Mockito.doNothing().when(frontendSession.writeAndFlush(Mockito.any()))
        mysqlSessionContext.setFrontSession(frontendSession);

//        ResponseHandler responseHandler = Mockito.spy(ResponseHandler.class);
//        Mockito.doNothing().when(responseHandler).okResponse(Mockito.any(OkPacket.class), Mockito.any(NettyBackendSession.class));

    }

    @Test
    public void testInsert(){

        String dataNodeName= "d0";
        String databaseName = "db0";
        String insert = "insert into tb0 values(1,1,1,'2016-01-01', '2016-01-01', 1)";

        // build route, ensuere host exists after route
        RouteResultsetNode node = new RouteResultsetNode(dataNodeName, insert, databaseName);
        RouteResultsetNode[] nodeArr = new RouteResultsetNode[]{node};
        RouteResultset routeResultset = new RouteResultset();
        routeResultset.setNodes(nodeArr);

        // whether should live in a cycle
        Host host = sessionService.getSession("d0", true);
        node.setHost(host);

        mysqlSessionContext.setRrs(routeResultset);

//        mysqlSessionContext

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
