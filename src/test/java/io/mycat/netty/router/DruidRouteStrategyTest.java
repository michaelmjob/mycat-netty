package io.mycat.netty.router;

import io.mycat.netty.conf.Configuration;
import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.parser.ServerParse;
import io.mycat.netty.router.DruidRouteStrategy;
import io.mycat.netty.router.RouteResultset;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/9/4.
 */
public class DruidRouteStrategyTest {
    private static final Logger logger = LoggerFactory.getLogger(DruidRouteStrategy.class);


    private  static MysqlSessionContext mysqlSessionContext;
    private  static DruidRouteStrategy druidRouteStrategy;

    @BeforeClass
    public static void beforeClass(){
        XMLSchemaLoader schemaLoader = new XMLSchemaLoader();
        schemaLoader.setSchemaFile("/DruidRouteStrategyConfig.xml");
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

        MysqlFrontendSession frontendSession = Mockito.mock(MysqlFrontendSession.class);
        Mockito.when(frontendSession.isAutocommit()).thenReturn(true);
        Mockito.when(frontendSession.getSchema()).thenReturn("frontdb0");
        Mockito.when(frontendSession.getCharset()).thenReturn("utf8");
        mysqlSessionContext = new MysqlSessionContext(frontendSession);
        druidRouteStrategy = new DruidRouteStrategy();
    }

    @Test
    public void testSelect() throws SQLNonTransientException {
        // tb1 -> TB1
        String select = "select order_id, product_id, usr_id from tb0 where order_id=3";

        RouteResultset routeResultset = druidRouteStrategy.route(ServerParse.SELECT, select, mysqlSessionContext);
        // check result
        logger.info("normal select node size : {}", routeResultset.size());
        for (RouteResultsetNode node : routeResultset.getNodes()) {
            logger.info("node database  database : {}", node.getDatabase());
            logger.info("node database  datanode name : {}", node.getDataNodeName());
            logger.info("node database  host : {}", node.getHost());  // should null, not route 2 selection
            logger.info("node database  sql : {}", node.getSql());    //
            logger.info("node database  canRunSlave : {}", node.getCanRunSlave());
        }


        // test range method, multi datanodes
        String select_range = "select order_id, product_id, usr_id from tb0 where order_id in (1,3, 5)";
        String select_btw = "select order_id, product_id, usr_id from tb0 where order_id between 1 and 5";

        routeResultset = druidRouteStrategy.route(ServerParse.SELECT, select_range, mysqlSessionContext);
        // check result
        logger.info("in node size : {}", routeResultset.size());
        for (RouteResultsetNode node : routeResultset.getNodes()) {
            logger.info("in node database  database : {}", node.getDatabase());
            logger.info("in node database  datanode name : {}", node.getDataNodeName());
            logger.info("in node database  host : {}", node.getHost());  // should null, not route 2 selection
            logger.info("in node database  sql : {}", node.getSql());    //
            logger.info("in node database  canRunSlave : {}", node.getCanRunSlave());
        }

        routeResultset = druidRouteStrategy.route(ServerParse.SELECT, select_btw, mysqlSessionContext);
        // check result
        logger.info("btw node size : {}", routeResultset.size());
        for (RouteResultsetNode node : routeResultset.getNodes()) {
            logger.info("btw node database  database : {}", node.getDatabase());
            logger.info("btw node database  datanode name : {}", node.getDataNodeName());
            logger.info("btw node database  host : {}", node.getHost());  // should null, not route 2 selection
            logger.info("btw node database  sql : {}", node.getSql());    //
            logger.info("btw node database  canRunSlave : {}", node.getCanRunSlave());
        }


        // select abnormal
        // no partition key
        String select_wrong = "select order_id, product_id, usr_id from tb0";
        try {
            druidRouteStrategy.route(ServerParse.SELECT, select_wrong, mysqlSessionContext);
            Assert.assertTrue("should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
            // do nothing
            logger.info("successfully catch exception from non partition key");
        }

        // select abnormal : table name not exist
        // throw exception : java.sql.SQLNonTransientException
        String select_nonTb = "select order_id, product_id, usr_id from tb5 where order_id=3";
        try {
            druidRouteStrategy.route(ServerParse.SELECT, select_nonTb, mysqlSessionContext);
            Assert.assertTrue("should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException ioe) {
            logger.info("successfully catch exception from non table");
        }
    }

//    插入需要全局自增ID
//    @Test
    public void testInsert() throws SQLNonTransientException {
        // tb1 -> TB1
        // 因为不知道表结构，所以必须标注 partitionKey
        String insert = "insert into tb0(order_id, product_id, usr_id, begin_time, end_time, status) values(3,1,1,'2016-01-01', '2016-01-01', 1)";

        RouteResultset routeResultset = druidRouteStrategy.route(ServerParse.INSERT, insert, mysqlSessionContext);
        // check result
        logger.info("node size : {}", routeResultset.size());
        for (RouteResultsetNode node : routeResultset.getNodes()) {
            logger.info("node database  database : {}", node.getDatabase());
            logger.info("node database  datanode name : {}", node.getDataNodeName());
            logger.info("node database  host : {}", node.getHost());  // should null, not route 2 selection
            logger.info("node database  sql : {}", node.getSql());    //
            logger.info("node database  canRunSlave : {}", node.getCanRunSlave());
        }


        // non table
        String insert_nontb = "insert into tb5 values(3,1,1,'2016-01-01', '2016-01-01', 1)";
        try {
            druidRouteStrategy.route(ServerParse.INSERT, insert_nontb, mysqlSessionContext);
            Assert.assertTrue("non table name should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
            // do nothing
            logger.info("successfully catch exception from non table");
        }

        // select abnormal
        // no partition key
        String insert_nonkey = "insert into tb0 values(3,1,1,'2016-01-01', '2016-01-01', 1)";
        try {
            druidRouteStrategy.route(ServerParse.INSERT, insert_nonkey, mysqlSessionContext);
            Assert.assertTrue("non partition key should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
            // do nothing
            logger.info("successfully catch exception from non partition key");
        }
    }

//    @Test
    public void testUpdate() throws SQLNonTransientException {
        // tb1 -> TB1
        // 因为不知道表结构，所以必须标注 partitionKey
        String insert = "update tb0 set status=2 where order_id=1";

        RouteResultset routeResultset = druidRouteStrategy.route(ServerParse.INSERT, insert, mysqlSessionContext);
        // check result
        logger.info("node size : {}", routeResultset.size());
        for (RouteResultsetNode node : routeResultset.getNodes()) {
            logger.info("node database  database : {}", node.getDatabase());
            logger.info("node database  datanode name : {}", node.getDataNodeName());
            logger.info("node database  host : {}", node.getHost());  // should null, not route 2 selection
            logger.info("node database  sql : {}", node.getSql());    //
            // 这个得具体的处理一下
            logger.info("node database  canRunSlave : {}", node.getCanRunSlave());
        }


        // non table
        String update_nontb = "update tb5 set status=2";
        try {
            druidRouteStrategy.route(ServerParse.UPDATE, update_nontb, mysqlSessionContext);
            Assert.assertTrue("non table name should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
            // do nothing
            logger.info("successfully catch exception from non table");
        }

        // select abnormal
        // no partition key
        String update_nonkey = "update tb0 set status=2";
        try {
            druidRouteStrategy.route(ServerParse.UPDATE, update_nonkey, mysqlSessionContext);
            Assert.assertTrue("non partition key should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
            // do nothing
            logger.info("successfully catch exception from non partition key");
        }
    }

//    @Test
    public void testDelete() throws SQLNonTransientException {
        // tb1 -> TB1
        // 因为不知道表结构，所以必须标注 partitionKey
        String insert = "delete from tb0 where order_id=1";

        RouteResultset routeResultset = druidRouteStrategy.route(ServerParse.DELETE, insert, mysqlSessionContext);
        // check result
        logger.info("node size : {}", routeResultset.size());
        for (RouteResultsetNode node : routeResultset.getNodes()) {
            logger.info("node database  database : {}", node.getDatabase());
            logger.info("node database  datanode name : {}", node.getDataNodeName());
            logger.info("node database  host : {}", node.getHost());  // should null, not route 2 selection
            logger.info("node database  sql : {}", node.getSql());    //
            // 这个得具体的处理一下
            logger.info("node database  canRunSlave : {}", node.getCanRunSlave());
        }


        // non table
        String update_nontb = "delete from tb5 where order_id=1";
        try {
            druidRouteStrategy.route(ServerParse.DELETE, update_nontb, mysqlSessionContext);
            Assert.assertTrue("non table name should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
            // do nothing， 这里 deleteparser 进行了拦截， 应该放在routeUtil里面进行拦截
            logger.info("successfully catch exception from non table");
        }

        // select abnormal
        // no partition key
        String update_nonkey = "delete from tb0";
        try {
            druidRouteStrategy.route(ServerParse.DELETE, update_nonkey, mysqlSessionContext);
            Assert.assertTrue("non partition key should throw IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
            // do nothing
            logger.info("successfully catch exception from non partition key");
        }
    }

}
