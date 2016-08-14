package io.mycat.netty.mysql;

import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import io.mycat.netty.util.SysProperties;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by snow_young on 16/8/13.
 */
@Data
public class MysqlSessionContext {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSessionContext.class);

    // session interface should ultilize
    private MySQLSession frontSession;
//    private NettyBackendSession backendSession;
    private ConcurrentHashMap<RouteResultsetNode, NettyBackendSession> target;

    private RouteResultset rrs;

    public MysqlSessionContext(MySQLSession frontSession){
        this.frontSession = frontSession;
        this.target = new ConcurrentHashMap<RouteResultsetNode, NettyBackendSession>(2, 0.75f);
    }

    public void releaseConnections(){
        for(RouteResultsetNode node : target.keySet()){
            releaseConnection(node);
        }
    }

    public void write(byte[] packet){
        ByteBuf out = this.frontSession.getChannel().alloc().buffer(SystemConfig.DEFAULT_BUFFER_SIZE);
        out.writeBytes(packet);
        this.frontSession.getChannel().writeAndFlush(out);
        this.frontSession.getCtx().writeAndFlush(out);
    }

    public void releaseConnection(RouteResultsetNode node){
        NettyBackendSession session = target.remove(node);
        if(!Objects.isNull(session)){
            // return back connection

        }
    }

    public void closeAndClearResources(){
        for(NettyBackendSession session : target.values()){
            // node.close(reason);
        }
        target.clear();
        // clearHandlesResources();
    }

    public boolean isClosed() {
        return frontSession.isClosed();
    }
}
