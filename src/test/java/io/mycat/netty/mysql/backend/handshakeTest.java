package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.backend.handler.BlockingResponseHandler;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.packet.CharsetUtil;
import io.mycat.netty.mysql.packet.HandshakePacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.packet.RowDataPacket;
import io.mycat.netty.mysql.proto.Packet;
import io.mycat.netty.mysql.proto.RowPacket;
import io.netty.channel.Channel;
import junit.framework.Assert;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.ws.Response;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * Created by snow_young on 16/8/13.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class handshakeTest {
    private static final Logger logger = LoggerFactory.getLogger(handshakeTest.class);

    private static NettyBackendSession session;

    @BeforeClass
    public static void beforeClass(){
        session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
//        session.setUserName("xujianhai");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setPort(3306);

        ResponseHandler responseHandler = Mockito.spy(ResponseHandler.class);
        Mockito.doNothing().when(responseHandler).okResponse(Mockito.any(OkPacket.class), Mockito.any(NettyBackendSession.class));
        session.setResponseHandler(responseHandler);

        session.initConnect();

        // for other operation
        session.setCharset("utf8");

    }

    @AfterClass
    public static void afterClass(){
        session.setClosed(true);
    }

    private CountDownLatch countDownLatch;
    @Before
    public void setUp(){
        countDownLatch = new CountDownLatch(1);
        session.setResponseHandler(new BlockingResponseHandler(countDownLatch));
    }

    @Test
    public void test_1_Databases() throws InterruptedException {

        session.sendQueryCmd("show databases");

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        countDownLatch.await();
        logger.info("finish connect, resultsetPacket {}", session.getResultSetPacket().getPacket());
        Assert.assertNull(session.getErrorPacket());
    }

    @Test
    public void test_2__USEDB() throws InterruptedException {

        session.sendQueryCmd("use  db0");

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        countDownLatch.await();

        logger.info("finish connect, resultsetPacket {}", session.getResultSetPacket().getPacket());
        Assert.assertNull(session.getErrorPacket());
    }

    @Test
    public void test_3_ShowTables() throws InterruptedException {
        logger.info("begin connect show tables");


        logger.info("begin show tables");
        session.sendQueryCmd("show tables");

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        countDownLatch.await();
        ROWOutput(session.getResultSetPacket().getRows());

        Assert.assertNull(session.getErrorPacket());
    }

    // each test, should remove tables and databases;
    @Test
    public void test_4_Insert() throws InterruptedException {
        String sql = "insert into mytable(t_title, t_author) values('i_title', 'i_author');";

        logger.info("begin insert table");
        session.sendQueryCmd(sql);

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        countDownLatch.await();
        OKOutput(session.getOkPacket());
        Assert.assertNull(session.getErrorPacket());
    }

    private void ROWOutput(List<RowDataPacket> rows){
        logger.info("length : {}", rows.size());
        for(RowDataPacket row : rows){
            StringBuilder builder = new StringBuilder();
            for(byte[] field : row.fieldValues){
                builder.append(new String(field)).append(" ,");
            }
            logger.info("field value : {}", builder.toString());
        }
    }

    @Test
    public void test_5_Select() throws InterruptedException {
        logger.info("begin connect select * from mytable");

        session.sendQueryCmd("select * from mytable");

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        countDownLatch.await();

        ROWOutput(session.getResultSetPacket().getRows());

        Assert.assertNull(session.getErrorPacket());
    }



    @Test
    public void test_6_Update() throws InterruptedException {
        String sql = "update  mytable set t_author=\'mysql_proxy\' where t_title=\'mysql_proxy\'";


        logger.info("begin update table");
        session.sendQueryCmd(sql);

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        countDownLatch.await();

        OKOutput(session.getOkPacket());
//        logger.info("finish update ok connect : {}", session.getOkPacket().getPacket());
        Assert.assertNull(session.getErrorPacket());
    }



    // 有时候会导致异常， 得添加重试
    @Test
    public void test_7_Delete() throws InterruptedException {
        String sql = "delete from mytable where t_title='i_title'";

        session.sendQueryCmd(sql);

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        countDownLatch.await();

        OKOutput(session.getOkPacket());
        Assert.assertNull(session.getErrorPacket());
    }


    public void OKOutput(OkPacket okPacket){
        logger.info("affectedRows : {}", okPacket.affectedRows);
        logger.info("insertId : {}", okPacket.insertId);
        logger.info("serverStatus : {}", okPacket.serverStatus);
        logger.info("warningCount : {}", okPacket.warningCount);
        if(!Objects.isNull(okPacket.message)) {
            logger.info("message : {}", new String(okPacket.message));
        }
    }
}
