package io.mycat.netty.mysql.backend.handler;


import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.router.RouteResultset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by snow_young on 16/8/13.
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeQueryHandler.class);

    private boolean prepared;
    protected byte packetId;

    private MysqlSessionContext sessionContext;

    // should be modifield : update/insert/delete no resultsetpacket
    // select resultsetpacket
    private OkPacket ok = new OkPacket();
    private ErrorPacket error = new ErrorPacket();
    private ResultSetPacket result = new ResultSetPacket();

    // limit N,M
    private int limitStart;
    private int limitSize;

    private AtomicInteger nodeCount;

    public MultiNodeQueryHandler(int sqlType, RouteResultset rrs, boolean autocommit, MysqlSessionContext sessionContext){

        this.sessionContext = sessionContext;
        nodeCount = new AtomicInteger(rrs.getNodes().length);
    }

    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        // can be stoppped about other session
    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {

        lock.lock();
        try {
            ok.affectedRows += packet.affectedRows;
            if (packet.insertId > 0) {
                ok.insertId = (ok.insertId == 0) ? packet.insertId : Math.min(ok.insertId, packet.insertId);
            }
        }finally{
            lock.unlock();
        }

        // ahout auto commit  implementation?
        if(decrementCountBy()){
            if(this.sessionContext.getFrontSession().isAutocommit()){
                // TODO: add rollback action
                sessionContext.releaseConnections();
            }

            if(this.isFailed() || sessionContext.isClosed()){
                tryErrorFinished();
                return;
            }

            lock.lock();
            try{
                ok.packetId = ++packetId;
                ok.serverStatus = sessionContext.getFrontSession().isAutocommit() ? 2 : 1;

                sessionContext.write(ok.getPacket());

            }finally{

            }

        }

    }

    // 处理失败的异常
    protected void tryErrorFinished(){
        if(!sessionContext.isClosed()){
            if(sessionContext.getFrontSession().isAutocommit()){
                sessionContext.closeAndClearResources();
            }else{
                // 非自动提交的处理
            }
        }
    }


    protected boolean decrementCountBy(){
        // TODO: ADD zero callback
        lock.lock();
        try{
            return nodeCount.decrementAndGet() == 0;
        }finally {
            lock.unlock();
        }
    }


    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {

    }



}
