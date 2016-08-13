package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.Capabilities;
import io.mycat.netty.mysql.MySQLProtocolDecoder;
import io.mycat.netty.mysql.packet.AuthPacket;
import io.mycat.netty.mysql.packet.HandshakePacket;
import io.mycat.netty.mysql.proto.Handshake;
import io.mycat.netty.mysql.proto.HandshakeResponse;
import io.mycat.netty.util.SecurityUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.SocketHandler;

/**
 * Created by snow_young on 16/8/12.
 */
@NoArgsConstructor
@Data
public class NettyBackendSession implements BackendSession{
    private static final Logger logger = LoggerFactory.getLogger(NettyBackendSession.class);

    private static NioEventLoopGroup eventLoop = new NioEventLoopGroup(8);

    private static final long CLIENT_FLAGS = initClientFlags();
    private volatile long lastTime;
    private volatile String schema = null;
    private volatile String oldSchema;
    private volatile boolean borrowed = false;
    private volatile boolean modifiedSQLExcluded = false;
    private volatile int batchCmdCount = 0;

    private String currentDB;
    private Channel serverChannel;
    private long connectionId;
    protected int packetHeaderSize;
    protected int maxPacketSize;
    protected volatile String charset;
    protected volatile int charsetIndex;
    private HandshakePacket handshake;

    private String userName;
    private String password;

    // maybe refactor with new class including host and port
    private String host;
    private int port;

    // dirty implementation: to ensure the connection is established before server serve.
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private volatile long since = System.currentTimeMillis();

    public NettyBackendSession(String host, int port){
        this.host = host;
        this.port = port;
    }



    // so, what aboud aboundant failures
    private void waitChannel(int loginTimeout){
        try{
            this.countDownLatch.await(loginTimeout, TimeUnit.MILLISECONDS);
        }catch (InterruptedException e) {
            logger.error("connect error : wait channel interrupted", e);
        }
    }

    @Override
    public String getCurrentDB() {
        return null;
    }

    public void sendBytes(byte[] bytes){
        this.serverChannel.writeAndFlush(bytes);
    }





    // ===================================== for login =====================================
    private static long initClientFlags(){
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = false;
//        boolean usingCompress=MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
        if(usingCompress)
        {
            flag |= Capabilities.CLIENT_COMPRESS;
        }
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;

    }

    public void initConnect(){
        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .group(eventLoop)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline p = socketChannel.pipeline();
                        p.addLast(new MySQLProtocolDecoder(), new MysqlHandshakeHandler(NettyBackendSession.this), new MysqlResponseHandler());
                    }
                });
        ChannelFuture f = b.connect(this.host, this.port);
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if(channelFuture.isSuccess()){
                    if(channelFuture.isSuccess()){
                        NettyBackendSession.this.serverChannel = channelFuture.channel();
                        logger.info("connect success");
                    }else{
                        logger.error("initial connection failed!");
                    }
                    NettyBackendSession.this.countDownLatch.countDown();
                }
            }
        });
        waitChannel(3000);
    }

    public byte[] authenticate() {
        // TODO: should utilize  HandshakeResponse + Auth
        AuthPacket packet = new AuthPacket();
        packet.packetId = 1;
        packet.clientFlags = CLIENT_FLAGS;
        packet.maxPacketSize = maxPacketSize;
        packet.charsetIndex = this.charsetIndex;
        packet.user = this.userName;
        try {
            packet.password = passwd(password, handshake);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
        packet.database = schema;

        logger.info("netty backend session begin to authenticate ");

        return packet.getPacket();
    }

    // TODO: mysql authenticate use two seed, but i use one, should utilize one
    // which flag is important
    private static byte[] passwd(String pass, HandshakePacket hs)
            throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        byte[] passwd = pass.getBytes();
        int sl1 = hs.seed.length;
        int sl2 = hs.restOfScrambleBuff.length;
        byte[] seed = new byte[sl1 + sl2];
        System.arraycopy(hs.seed, 0, seed, 0, sl1);
        System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
        return SecurityUtil.scramble411(passwd, seed);
    }

    // ===================================== ???  =====================================
    @Override
    public String toString() {
        return "";
//        return "MySQLConnection [id=" + id + ", lastTime=" + lastTime
//                + ", user=" + user
//                + ", schema=" + schema + ", old shema=" + oldSchema
//                + ", borrowed=" + borrowed + ", fromSlaveDB=" + fromSlaveDB
//                + ", threadId=" + threadId + ", charset=" + charset
//                + ", txIsolation=" + txIsolation + ", autocommit=" + autocommit
//                + ", attachment=" + attachment + ", respHandler=" + respHandler
//                + ", host=" + host + ", port=" + port + ", statusSync="
//                + statusSync + ", writeQueue=" + this.getWriteQueue().size()
//                + ", modifiedSQLExecuted=" + modifiedSQLExecuted + "]";
    }

    public static class Builder{

    }

}
