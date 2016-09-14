package io.mycat.netty.mysql.backend;

import io.mycat.netty.TestConstants;
import io.mycat.netty.util.TestUtil;
import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.handler.BlockingResponseHandler;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import junit.framework.Assert;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by snow_young on 16/8/13.
 * <p>
 * session 级别的测试
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HandshakeTest {
    private static final Logger logger = LoggerFactory.getLogger(HandshakeTest.class);

    private static NettyBackendSession session;


    @BeforeClass
    public static void beforeClass() throws InterruptedException, SQLException {
//        H2TestUtil.beforeClass();

        session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName(TestConstants.user);
        session.setPassword(TestConstants.pass);
        String ip = TestConstants.db0url.split(":")[0];
        int port = Integer.parseInt(TestConstants.db0url.split(":")[1]);
        session.setHost(ip);
        session.setPort(port);

        // what about spy method
        CountDownLatch countDownLatch = new CountDownLatch(1);
        session.setResponseHandler(new BlockingResponseHandler(countDownLatch));
        session.initConnect();

        countDownLatch.await();
        logger.info("handshake success");
        Host host = Mockito.mock(Host.class);
        Mockito.doNothing().when(host).back(Mockito.anyLong());

        session.setOwner(host);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        session.close();
//        session.setClosed(true);
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
            TestUtil.ROWOutput(packet.getRows());
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
            TestUtil.OKOutput((OkPacket) mySQLPacket);
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
            TestUtil.ROWOutput(packet.getRows());
        });

        countDownLatch.await();

        logger.info("countdown!");

        Assert.assertNull(session.getErrorPacket());
    }

    // each test, should remove tables and databases;
    @Test
    public void test_4_Insert() throws InterruptedException {
        String sql = "insert into tb0 values(2,2,2,'2016-01-01', '2016-01-01', 1)";
        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof OkPacket;
            TestUtil.OKOutput((OkPacket) mySQLPacket);
            Assert.assertNull(session.getErrorPacket());
        });

        logger.info("begin insert table");
        session.sendQueryCmd(sql);

        countDownLatch.await();

    }



    @Test
    public void test_5_Select() throws InterruptedException {
        logger.info("begin connect select * from tb0");

        // 单个运行，无数据，是正常的
        // 有数据也是正常的
        blockingResponseHandler.setCheck(mySQLPacket -> {
            ResultSetPacket packet = (ResultSetPacket)mySQLPacket;
            TestUtil.ROWOutput(packet.getRows());

            Assert.assertNull(session.getErrorPacket());
        });

        // just for test
         session.sendQueryCmd("select * from tb0");


        countDownLatch.await();

        logger.info("countdown!");
    }


    @Test
    public void test_6_Update() throws InterruptedException {
        String sql = "update tb0 set status=2 where order_id=2";

        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof OkPacket;
            TestUtil.OKOutput((OkPacket) mySQLPacket);
            Assert.assertNull(session.getErrorPacket());
        });


        logger.info("begin update table");
        session.sendQueryCmd(sql);

        countDownLatch.await();

    }



    // 有时候会导致异常， 得添加重试
    @Test
    public void test_7_Delete() throws InterruptedException {
        String sql = "delete from tb0 where order_id=2";

        blockingResponseHandler.setCheck(mySQLPacket -> {
            assert mySQLPacket instanceof OkPacket;
            TestUtil.OKOutput((OkPacket) mySQLPacket);
            Assert.assertNull(session.getErrorPacket());
        });

        session.sendQueryCmd(sql);

        countDownLatch.await();

    }

}
