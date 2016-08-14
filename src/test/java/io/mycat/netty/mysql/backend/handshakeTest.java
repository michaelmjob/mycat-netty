package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.packet.HandshakePacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.packet.RowDataPacket;
import io.mycat.netty.mysql.proto.RowPacket;
import io.netty.channel.Channel;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

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

        logger.info("finish connect, resultsetPacket {}", session.getResultSetPacket().getPacket());
        Assert.assertNull(session.getErrorPacket());
    }


    @Test
    public void testUseDB(){
        logger.info("begin connect show tables");
        NettyBackendSession session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
//        session.setUserName("xujianhai");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setCurrentDB("mydb");
        session.setPort(3306);

        session.initConnect();

        logger.info("begin show tables");
        session.sendQueryCmd("show tables");

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ROWOutput(session.getResultSetPacket().getRows());

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
    public void testSelect(){
        logger.info("begin connect select * from mytable");
        NettyBackendSession session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
//        session.setUserName("xujianhai");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setCurrentDB("mydb");
        session.setPort(3306);

        session.initConnect();

        logger.info("begin select * from mytable");
        session.sendQueryCmd("select * from mytable");

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ROWOutput(session.getResultSetPacket().getRows());

        Assert.assertNull(session.getErrorPacket());
    }



    @Test
    public void testUpdate(){
        String sql = "update  mytable set t_author=\'mysql_proxy\' where t_title=\'mysql_proxy\'";

        NettyBackendSession session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
//        session.setUserName("xujianhai");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setCurrentDB("mydb");
        session.setPort(3306);

        session.initConnect();

        logger.info("begin update table");
        session.sendQueryCmd(sql);

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        OKOutput(session.getOkPacket());
//        logger.info("finish update ok connect : {}", session.getOkPacket().getPacket());
        Assert.assertNull(session.getErrorPacket());
    }



    // each test, should remove tables and databases;
    @Test
    public void testInsert(){
        String sql = "insert into mytable(t_title, t_author) values('i_title', 'i_author');";

        NettyBackendSession session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
//        session.setUserName("xujianhai");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setCurrentDB("mydb");
        session.setPort(3306);

        session.initConnect();

        logger.info("begin insert table");
        session.sendQueryCmd(sql);

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        OKOutput(session.getOkPacket());
        Assert.assertNull(session.getErrorPacket());
    }

    // 有时候会导致异常， 得添加重试
    @Test
    public void testDelete(){
        String sql = "delete from mytable where t_title='i_title'";

        NettyBackendSession session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);
        session.setUserName("root");
//        session.setUserName("xujianhai");
        session.setPassword("xujianhai");
        session.setHost("localhost");
        session.setCurrentDB("mydb");
        session.setPort(3306);

        session.initConnect();

        logger.info("begin delete from table");
        session.sendQueryCmd(sql);

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
