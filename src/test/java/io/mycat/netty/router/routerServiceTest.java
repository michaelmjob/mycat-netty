package io.mycat.netty.router;

import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import io.mycat.netty.mysql.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/8/29.
 */
public class RouterServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(RouterServiceTest.class);


    protected static String fileName = "/SessionServiceTest.xml";


    @BeforeClass
    public void beforeClass(){
//        XMLSchemaLoader schemaLoader = new XMLSchemaLoader();
//        schemaLoader.setSchemaFile(fileName);
//
//        schemaLoader.load();



    }

    public void Selecttest() throws SQLNonTransientException {

        String insert = "";
        String delete = "";
        String update = "";
        String select = "";

        // move to constant

        MysqlFrontendSession frontendSession = Mockito.mock(MysqlFrontendSession.class);


        MysqlSessionContext mysqlSessionContext = new MysqlSessionContext(frontendSession);


        RouteResultset routeResultset = RouteService.route(ServerParse.SELECT , select, mysqlSessionContext);



        routeResultset = RouteService.route(ServerParse.INSERT , insert, mysqlSessionContext);
        routeResultset = RouteService.route(ServerParse.DELETE , delete, mysqlSessionContext);
        routeResultset = RouteService.route(ServerParse.UPDATE , update, mysqlSessionContext);

    }
}
