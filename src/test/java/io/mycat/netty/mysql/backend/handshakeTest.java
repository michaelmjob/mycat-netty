package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.packet.HandshakePacket;
import io.mycat.netty.mysql.packet.RowDataPacket;
import io.mycat.netty.mysql.proto.RowPacket;
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


        logger.info("begin show databases");
        session.sendQueryCmd("show databases");

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(RowDataPacket row : session.getResultSetPacket().getRows()){
            logger.info("rows");
            for(byte[] field : row.fieldValues){
                logger.info("field : {}", field);
            }
        }


        logger.info("finish connect");


    }
}
