package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by snow_young on 16/8/31.
 */
public class BackendTest {
    private static final Logger logger = LoggerFactory.getLogger(BackendTest.class);

    protected static String fileName = "/SessionServiceTest.xml";

    protected static SessionService sessionService;



    public static void init() throws ParserConfigurationException, SAXException, IOException {
        XMLSchemaLoader schemaLoader = new XMLSchemaLoader();
        schemaLoader.setSchemaFile(fileName);

        schemaLoader.load();

        sessionService = new SessionService();
        sessionService.load_datasource(schemaLoader.getDatasource(), schemaLoader.getSchemaConfigs().values());
        sessionService.init_datasource();

        checkConsistency(SessionService.getDataSources().get("d0"), "db0",2);
        checkConsistency(SessionService.getDataSources().get("d1"), "db1", 2);

        // logic
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

    private static void checkConsistency(DataSource dataSource, String dbname, int size){
        for(Host host : dataSource.getAllHosts()){
            int truesize = host.getConMap().getSchemaConQueue(dbname).getConnQueue(true).size();
            int falsesize = host.getConMap().getSchemaConQueue(dbname).getConnQueue(false).size();
            logger.info(" true size : {}", truesize);
            logger.info(" false size : {}", falsesize);
            junit.framework.Assert.assertEquals(size, truesize);
            junit.framework.Assert.assertEquals(0, falsesize);
//            junit.framework.Assert.assertEquals(size, d1truesize);
        }
    }
}
