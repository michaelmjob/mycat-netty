package io.mycat.netty.router.parser.druid;

import io.mycat.netty.conf.Configuration;
import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.parser.ServerParse;
import io.mycat.netty.router.DruidRouteStrategy;
import io.mycat.netty.router.RouteResultset;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/9/4.
 */
public class DruidRouteStrategyTest {

    @Test
    public void testSQL() throws SQLNonTransientException {
        String insert = "";
        String delete = "";
        String update = "";
//        String select = "select order_id, product_id, usr_id from tb0 where begin_time='2016-01-01'";
        String select = "select order_id, product_id, usr_id from tb1 where order_id=3";

        // set xml configuration
//        Configuration configuration = Mockito.mock(Configuration.class);
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
        Mockito.when(frontendSession.getSchema()).thenReturn("");
        Mockito.when(frontendSession.getCharset()).thenReturn("utf8");
        MysqlSessionContext mysqlSessionContext = new MysqlSessionContext(frontendSession);

        DruidRouteStrategy druidRouteStrategy = new DruidRouteStrategy();
        RouteResultset routeResultset = druidRouteStrategy.route(ServerParse.SELECT, select, mysqlSessionContext);

        // check result


    }
}
