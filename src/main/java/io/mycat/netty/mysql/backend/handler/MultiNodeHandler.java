package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.router.RouteResultset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by snow_young on 16/8/13.
 */
public abstract class MultiNodeHandler extends AbstractResponseHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeHandler.class);

    protected final ReentrantLock lock = new ReentrantLock();
    protected AtomicBoolean isFailed = new AtomicBoolean(false);
    // move 2 abs
    protected MysqlSessionContext mysqlSessionContext;
    protected byte packetId;
    protected final AtomicBoolean errorRepsponsed = new AtomicBoolean(false);
    protected volatile StringBuilder errorMsg;
    protected RouteResultset rrs;

    public MultiNodeHandler(RouteResultset rrs, MysqlSessionContext mysqlSessionContext){
        this.rrs = rrs;
        this.mysqlSessionContext = mysqlSessionContext;
    }

    public boolean isFailed(){
        return isFailed.get();
    }

}
