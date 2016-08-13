package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.packet.HandshakePacket;
import io.netty.channel.Channel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/8/13.
 */
public class handshakeTest {
    private static final Logger logger = LoggerFactory.getLogger(handshakeTest.class);

    @Test
    public void testConnection(){
        logger.info("start connect");
        NettyBackendSession session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
//        session.setUserName("xujianhai");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setPort(3306);

        session.initConnect();

        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        session.send

        logger.info("finish connect");


    }
}
