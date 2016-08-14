package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by snow_young on 16/8/13.
 */
public abstract class MultiNodeHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeHandler.class);

    protected final ReentrantLock lock = new ReentrantLock();
    private AtomicBoolean isFailed = new AtomicBoolean(false);

    public MultiNodeHandler(){

    }

    public boolean isFailed(){
        return isFailed.get();
    }

//    @Override
//    public void errorResponse(ErrorPacket packet, MysqlSessionContext session) {
//
//    }
//
//    @Override
//    public void okResponse(OkPacket packet, MysqlSessionContext session) {
//
//    }
//
//    @Override
//    public void resultsetResponse(ResultSetPacket resultSetPacket, MysqlSessionContext session) {
//
//    }
}
