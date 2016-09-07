package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.handler.BlockingResponseHandler;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.packet.RowDataPacket;
import io.mycat.netty.mysql.proto.Packet;
import io.mycat.netty.mysql.proto.RowPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.netty.channel.Channel;
import junit.framework.Assert;
import org.junit.*;
import org.junit.runner.Result;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * Created by snow_young on 16/8/13.
 * <p>
 * session 级别的测试
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class handshakeTest {
    private static final Logger logger = LoggerFactory.getLogger(handshakeTest.class);

    private static NettyBackendSession session;
    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setPort(3306);

        // 先使用 session服用的模式
//        ResponseHandler responseHandler = Mockito.spy(ResponseHandler.class);
//        Mockito.doNothing().when(responseHandler).okResponse(Mockito.any(OkPacket.class), Mockito.any(NettyBackendSession.class));
//        session.setResponseHandler(responseHandler);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        session.setResponseHandler(new BlockingResponseHandler(countDownLatch));
        session.initConnect();

        countDownLatch.await();
        logger.info("handshake success");
        // add mokito 4 back funciton
        // can mock part of function
//        Host host = new EmptyHost();
        Host host = Mockito.mock(Host.class);
        Mockito.doNothing().when(host).back(Mockito.anyLong());

        session.setOwner(host);
    }

    @AfterClass
    public static void afterClass() {
        session.setClosed(true);
    }

    private CountDownLatch countDownLatch;

    private static BlockingResponseHandler blockingResponseHandler;
    @Before
    public void setUp() {
        countDownLatch = new CountDownLatch(1);

        blockingResponseHandler = new BlockingResponseHandler(countDownLatch);
        session.setResponseHandler(blockingResponseHandler);
    }

    @Test
    public void test_1_Databases() throws InterruptedException {

        logger.info("show databases");
        session.sendQueryCmd("show databases");

        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof ResultSetPacket;
            ResultSetPacket packet = (ResultSetPacket)mySQLPacket;
            ROWOutput(packet.getRows());
            Assert.assertNull(session.getErrorPacket());
        });

        countDownLatch.await();

        logger.info("countdown!");

    }

    @Test
    public void test_2__USEDB() throws InterruptedException {

        logger.info("use db0");
        session.sendQueryCmd("use  db0");

        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof OkPacket;
            OKOutput((OkPacket)mySQLPacket);
            Assert.assertNull(session.getErrorPacket());
        });

        countDownLatch.await();

        logger.info("countdown!");
    }

    @Test
    public void test_3_ShowTables() throws InterruptedException {

        logger.info("begin show tables");
        session.sendQueryCmd("show tables");


        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof ResultSetPacket;
            ResultSetPacket packet = (ResultSetPacket)mySQLPacket;
            ROWOutput(packet.getRows());
        });

        countDownLatch.await();

        logger.info("countdown!");

        Assert.assertNull(session.getErrorPacket());
    }

    // each test, should remove tables and databases;
    @Test
    public void test_4_Insert() throws InterruptedException {
        String sql = "insert into mytable(t_title, t_author) values('i_title', 'i_author');";
        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof OkPacket;
            OKOutput((OkPacket) mySQLPacket);
            Assert.assertNull(session.getErrorPacket());
        });

        logger.info("begin insert table");
        session.sendQueryCmd(sql);

        countDownLatch.await();

    }



    @Test
    public void test_5_Select() throws InterruptedException {
        logger.info("begin connect select * from mytable");

        // 单个运行，无数据，是正常的
        // 有数据也是正常的
        blockingResponseHandler.setCheck(mySQLPacket -> {
            ResultSetPacket packet = (ResultSetPacket)mySQLPacket;
            ROWOutput(packet.getRows());

            Assert.assertNull(session.getErrorPacket());
        });

         session.sendQueryCmd("select * from mytable");


        countDownLatch.await();

        logger.info("countdown!");
    }


    @Test
    public void test_6_Update() throws InterruptedException {
        String sql = "update  mytable set t_author='mysql_proxy' where t_title='i_title'";

        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof OkPacket;
            OKOutput((OkPacket)mySQLPacket);
            Assert.assertNull(session.getErrorPacket());
        });


        logger.info("begin update table");
        session.sendQueryCmd(sql);

        countDownLatch.await();

    }



    // 有时候会导致异常， 得添加重试
    @Test
    public void test_7_Delete() throws InterruptedException {
        String sql = "delete from mytable where t_title='i_title'";

        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof OkPacket;
            OKOutput((OkPacket) mySQLPacket);
            Assert.assertNull(session.getErrorPacket());
        });

        session.sendQueryCmd(sql);

        countDownLatch.await();

    }

    private void ROWOutput(List<RowDataPacket> rows) {
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
    public void OKOutput(OkPacket okPacket) {
        logger.info("affectedRows : {}", okPacket.affectedRows);
        logger.info("insertId : {}", okPacket.insertId);
        logger.info("serverStatus : {}", okPacket.serverStatus);
        logger.info("warningCount : {}", okPacket.warningCount);
        if (!Objects.isNull(okPacket.message)) {
            logger.info("message : {}", new String(okPacket.message));
        }
    }
}
