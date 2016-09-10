package io.mycat.netty.util;

import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.handler.SyncMysqlSessionContext;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.packet.RowDataPacket;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by snow_young on 16/9/7.
 */
public class TestUtil {
    private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);

    // output util
    public static void ROWOutput(List<RowDataPacket> rows) {
        logger.info("length : {}", rows.size());
        for (RowDataPacket row : rows) {
            StringBuilder builder = new StringBuilder();
            for (byte[] field : row.fieldValues) {
                logger.info("field value :  {}", field);
                builder.append(new String(field)).append(" ,");
            }
            logger.info("field value : {}", builder.toString());
        }
    }

    // 没有数据返回，就是null, 这里会报null指针异常，需要处理
    public static void OKOutput(OkPacket okPacket) {
        logger.info("affectedRows : {}", okPacket.affectedRows);
        logger.info("insertId : {}", okPacket.insertId);
        logger.info("serverStatus : {}", okPacket.serverStatus);
        logger.info("warningCount : {}", okPacket.warningCount);
        if (!Objects.isNull(okPacket.message)) {
            logger.info("message : {}", new String(okPacket.message));
        }
    }
    // end of output util


    // show util
    public static void showDB(Supplier<Connection> getConn) throws SQLException {
        Connection conn = getConn.get();
        logger.info("show databases");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("show databases");
        while (resultSet.next()) {
            logger.info(resultSet.getString(1));
        }
        conn.close();
    }

    public static void showTB(Supplier<Connection> getConn) throws SQLException {
        Connection conn = getConn.get();
        Statement statement = conn.createStatement();
        logger.info("show tables");
        ResultSet resultSet = statement.executeQuery("show tables");
        while (resultSet.next()) {
            logger.info(resultSet.getString(1));
        }
        conn.close();
    }
    // end of show util


    // for OneResponseHandler  MultiResponseHandler
    public static RouteResultset buildSingleRouteResultSet(String dataNodeName, String databaseName, String sql) {
        RouteResultsetNode node = new RouteResultsetNode(dataNodeName, databaseName, sql);
        RouteResultsetNode[] nodeArr = new RouteResultsetNode[]{node};
        RouteResultset routeResultset = new RouteResultset();
        routeResultset.setNodes(nodeArr);
        return routeResultset;
    }

    public static void testSQL(MysqlFrontendSession frontendSession, RouteResultset routeResultset, Consumer<MySQLPacket> check) throws InterruptedException {
        SyncMysqlSessionContext blockingMysqlSessionContext = new SyncMysqlSessionContext(frontendSession);

        blockingMysqlSessionContext.setRrs(routeResultset);
        blockingMysqlSessionContext.getSession();

        blockingMysqlSessionContext.setBlocking(new CountDownLatch(1));
        blockingMysqlSessionContext.setCheck(check);
        blockingMysqlSessionContext.setCurrentStatus(MysqlSessionContext.STATUS.RECEIVE);

        blockingMysqlSessionContext.send();

        blockingMysqlSessionContext.blocking();
        logger.info("sql send && receive finish");
    }

    public static RouteResultset buildSingleRouteResultSet(String[] dataNodeName, String[] databaseName, String[] sql) {
        RouteResultsetNode node_sql1 = new RouteResultsetNode(dataNodeName[0], databaseName[0], sql[0]);
        RouteResultsetNode node_sql2 = new RouteResultsetNode(dataNodeName[1], databaseName[1], sql[1]);
        RouteResultsetNode[] nodeArr = new RouteResultsetNode[]{node_sql1, node_sql2};
        RouteResultset routeResultset = new RouteResultset();
        routeResultset.setNodes(nodeArr);
        return routeResultset;
    }
    // end of OneResponseHandler  MultiResponseHandler
}
