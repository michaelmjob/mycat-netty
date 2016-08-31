package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.conf.XMLSchemaLoader;
import io.mycat.netty.mysql.MysqlFrontendSession;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.backend.handler.SingleNodeHandler;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.router.RouteResultset;
import jdk.nashorn.internal.runtime.regexp.joni.ast.BackRefNode;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.SchemaFactoryLoader;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by snow_young on 16/8/29.
 *
 create table tb0(
 order_id INT NOT NULL,
 product_id INT NOT NULL,
 usr_id INT NOT NULL,
 begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 status INT,
 PRIMARY KEY(order_id)
 );

 host 级别， sessionService 交互
 */
public class SessionServiceTest extends BackendTest{
    private static Logger logger = LoggerFactory.getLogger(SessionServiceTest.class);


    private static MysqlSessionContext mysqlSessionContext;

    @BeforeClass
    public static void beforeClass() throws ParserConfigurationException, SAXException, IOException {

        init();

        // mockito then
        MysqlFrontendSession frontendSession = new MysqlFrontendSession();
        mysqlSessionContext = new MysqlSessionContext(frontendSession);

        frontendSession.setSchema("db0");
        frontendSession.setAutocommit(true);
        mysqlSessionContext.setFrontSession(frontendSession);
    }


    // test insert && delete
    @Test
    public void testSend() throws IOException, InterruptedException {

        Host host = sessionService.getSession("d0", true);

        // frontend_db_name 换成 backend_db_name 需要更换
        // 这个已经是转换过的 sql senstence.
        // 分库分表， frontend_table -> backend_db_table,  table里面的内容不回发生改变。


        CountDownLatch countDownLatch;
        String sql;

        sql = "delete from  tb0 where order_id=1";
        countDownLatch = new CountDownLatch(1);
        host.send(sql, getResponseHandler(countDownLatch, host), mysqlSessionContext);
        countDownLatch.await();


        sql = "insert into tb0 values(1,1,1,'2016-01-01', '2016-01-01', 1)";
        countDownLatch = new CountDownLatch(1);
        host.send(sql, getResponseHandler(countDownLatch, host), mysqlSessionContext);
        countDownLatch.await();


        sql = "delete from  tb0 where order_id=1";
        countDownLatch = new CountDownLatch(1);
        host.send(sql, getResponseHandler(countDownLatch, host), mysqlSessionContext);
        countDownLatch.await();
    }


    public ResponseHandler getResponseHandler(CountDownLatch countDownLatch, Host host){
        return new ResponseHandler() {
            @Override
            public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
                logger.info("error Response  : {}", new String(packet.message));
                Assert.assertFalse(true);
                host.back(session, true);
                countDownLatch.countDown();
            }

            @Override
            public void okResponse(OkPacket packet, NettyBackendSession session) {
                logger.info("ok Response  : {}", packet.affectedRows);
                // return session
                host.back(session, true);
                countDownLatch.countDown();
            }

            @Override
            public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
                logger.info("result Response  : {}", resultSetPacket.getRows());
                Assert.assertFalse(true);
                host.back(session, true);
                countDownLatch.countDown();
            }

            @Override
            public void send() {
                logger.info("send message");
            }
        };
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

    public void testInit_datasource(){

    }
}
