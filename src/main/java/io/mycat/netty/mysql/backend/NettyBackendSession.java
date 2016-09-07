package io.mycat.netty.mysql.backend;

import com.sun.tools.internal.ws.wscompile.ErrorReceiver;
import io.mycat.netty.conf.Capabilities;
import io.mycat.netty.mysql.MySQLProtocolDecoder;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.packet.*;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.util.SecurityUtil;
import io.mycat.netty.util.TimeUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by snow_young on 16/8/12.
 */
@NoArgsConstructor
@Data
public class NettyBackendSession implements BackendSession {
    private static final Logger logger = LoggerFactory.getLogger(NettyBackendSession.class);

    private static NioEventLoopGroup eventLoop = new NioEventLoopGroup(8);
    public static final int DEFAULT_BUFFER_SIZE = 16384;

    private static final long CLIENT_FLAGS = initClientFlags();
    private volatile long lastTime;
    private volatile String oldSchema;
    private volatile boolean borrowed = false;
    private volatile boolean modifiedSQLExcluded = false;
    private volatile int batchCmdCount = 0;

    private String currentDB;
    private Channel serverChannel;
    private long connectionId;
    protected int packetHeaderSize;
    protected int maxPacketSize;
    protected volatile String charset = "utf8";
    protected volatile int charsetIndex = 33;
    private HandshakePacket handshake;

    private String userName;
    private String password;

    // maybe refactor with new class including host and port
    private String host;
    private int port;

    private Host owner;

    private boolean autocommit;

    // dirty implementation: to ensure the connection is established before server serve.
    @Setter
    @Getter
    private CountDownLatch countDownLatch;

    private long sessionId;

    private volatile long since = System.currentTimeMillis();

    private ResultSetPacket resultSetPacket = new ResultSetPacket();
    private OkPacket okPacket = null;
    private ErrorPacket errorPacket = null;

    private boolean isClosed = false;
    private AtomicBoolean isQuit = new AtomicBoolean(false);

    private ResponseHandler responseHandler;


    public boolean isClosedOrQuit() {
        return isClosed() || isQuit.get();
    }

    // session not remove originaly, dependent on Host
    public void back() {
        this.responseHandler = null;
        resultSetPacket.init();
        owner.back(sessionId);
    }

    // should invole responeHandler
    public void setOkPacket(byte[] ok) {
        this.okPacket = new OkPacket();
        this.okPacket.read(ok);
        responseHandler.okResponse(this.okPacket, this);
        this.back();
    }

    public void setErrorPacket(byte[] data) {
        this.errorPacket = new ErrorPacket();
        this.errorPacket.read(data);
        responseHandler.errorResponse(this.errorPacket, this);
        this.back();
    }

    public void setErrorPacket(ErrorPacket errorPacket) {
        responseHandler.errorResponse(errorPacket, this);
        this.back();
    }

    // resultSetPacket 这里依赖解析
    public void setFinished() {
        if (Objects.isNull(responseHandler)) {
            logger.error("resultsetPacket with sql:  {} loss responseHandler to respond", query);
            logger.error("result set packet : {}", resultSetPacket);
        } else {
            this.responseHandler.resultsetResponse(resultSetPacket, this);
        }
        back();
        query = "unknow";
    }

    public NettyBackendSession(String host, int port) {
        this.host = host;
        this.port = port;

        this.isQuit = new AtomicBoolean(false);
    }

    public void setUrl(String url) {

    }

    // so, what aboud aboundant failures
    private boolean waitChannel(int loginTimeout) {
        try {
            logger.info("wait countDownLatch");
            this.countDownLatch.await(loginTimeout, TimeUnit.MILLISECONDS);
            logger.info("wait countDownLatch success");
            return true;
        } catch (InterruptedException e) {
            logger.error("connect error : wait channel interrupted", e);
            return false;
        }
    }

    @Override
    public String getCurrentDB() {
        return this.currentDB;
    }

    public void sendBytes(byte[] bytes) {
        ByteBuf out = this.serverChannel.alloc().buffer(DEFAULT_BUFFER_SIZE);
        out.writeBytes(bytes);
        this.serverChannel.writeAndFlush(out);
    }

    public void sendPacket(MySQLPacket packet) {
        ByteBuf out = this.serverChannel.alloc().buffer(DEFAULT_BUFFER_SIZE);
        out.writeBytes(packet.getPacket());
        this.serverChannel.writeAndFlush(out);
    }

    private String query;

    // select/insert/delete/update
    public void sendQueryCmd(String query) {
        this.query = query;
        CommandPacket packet = new CommandPacket();
        packet.packetId = 0;
        packet.command = MySQLPacket.COM_QUERY;
        try {
            packet.arg = query.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            logger.error("get bytes from query occurs error", e);
            throw new RuntimeException(e);
        }
        lastTime = TimeUtil.currentTimeMillis();

        ByteBuf out = this.serverChannel.alloc().buffer(DEFAULT_BUFFER_SIZE);
        out.writeBytes(packet.getPacket());
        this.serverChannel.writeAndFlush(out);
        logger.info("send bytes for query : {}", query);
    }

    // ===================================== for login =====================================
    private static long initClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = false;
//        boolean usingCompress=MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
        if (usingCompress) {
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

    // what aboud init and create!!
    public boolean initConnect() {
        countDownLatch = new CountDownLatch(1);
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
                        p.addLast(new MySQLProtocolDecoder(),
                                new MysqlHandshakeHandler(NettyBackendSession.this),
                                new MysqlBackendProtocolHandler(NettyBackendSession.this));
                    }
                });
        ChannelFuture f = b.connect(this.host, this.port);
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isSuccess()) {
                    NettyBackendSession.this.serverChannel = channelFuture.channel();
                    logger.info("tcp connect to mysql success");
                } else {
                    logger.error("initial connection failed!");
                }
                NettyBackendSession.this.countDownLatch.countDown();
                logger.info("countdownLatch has been solved");
            }
        });
        return waitChannel(1000);
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
//        packet.database = schema;
        packet.database = this.currentDB;

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

    public static class Builder {

    }

}
