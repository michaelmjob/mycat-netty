package io.mycat.netty.mysql;

import io.mycat.netty.ServerArgs;
import io.mycat.netty.util.Constants;
import io.mycat.netty.NettyServer;
import io.mycat.netty.ProtocolHandler;
import io.netty.channel.ChannelHandler;

/**
 * Created by snow_young on 16/8/3.
 */
public class MySQLServer extends NettyServer {

    public static final String DEFAULT_CHARSET = "utf8";
    
    public static final byte PROTOCOL_VERSION = 10;

    public static final String VERSION_COMMENT = "Mycat based netty MySQL Protocol Server";
    public static final String SERVER_VERSION = "5.7.13" + VERSION_COMMENT + "-" + Constants.getFullVersion();

    public MySQLServer(ServerArgs args) {
        super(args);
    }


    @Override
    protected String getServerName() {
        return VERSION_COMMENT;
    }

    @Override
    protected ChannelHandler createProtocolDecoder() {
        return new MySQLProtocolDecoder();
    }
    
    @Override
    protected ProtocolHandler createProtocolHandler() {
        return new MySQLProtocolHandler();
    }
    
    @Override
    protected ProtocolHandler createHandshakeHandler() {
        return new MySQLHandshakeHandler();
    }
}
