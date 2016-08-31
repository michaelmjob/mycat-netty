package io.mycat.netty.mysql.handler;

import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.BackendTest;
import io.mycat.netty.mysql.backend.SessionService;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by snow_young on 16/8/21.
 */
public class MultiNodeHandlerTest extends BackendTest{
    private static Logger logger = LoggerFactory.getLogger(MultiNodeHandlerTest.class);

    private static MysqlSessionContext mysqlSessionContext;

    @BeforeClass
    public static void beforeClass() throws IOException, SAXException, ParserConfigurationException {
        init();

        // mockito then
        MysqlFrontendSession frontendSession = new MysqlFrontendSession();
        mysqlSessionContext = new MysqlSessionContext(frontendSession);

        frontendSession.setSchema("db0");
        frontendSession.setAutocommit(true);
        mysqlSessionContext.setFrontSession(frontendSession);
    }

    @Test
    public void testInsert(){

    }

    public void testDelete(){

    }

    public void testUpdate(){

    }

    public void testSelect(){

    }


}
