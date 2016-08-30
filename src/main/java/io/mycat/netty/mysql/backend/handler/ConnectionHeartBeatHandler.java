package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by snow_young on 16/8/14.
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {
    private static final Logger logger = LoggerFactory
            .getLogger(ConnectionHeartBeatHandler.class);
    protected final ReentrantLock lock = new ReentrantLock();
    private final ConcurrentHashMap<Long, HeartBeatCon> allCons = new ConcurrentHashMap<Long, HeartBeatCon>();

    public void doHeartBeat(NettyBackendSession conn, String sql) {
        if (logger.isDebugEnabled()) {
            logger.debug("do heartbeat for con " + conn);
        }
//        try {
//            HeartBeatCon hbCon = new HeartBeatCon(conn);
//            boolean notExist = (allCons.putIfAbsent(hbCon.conn.getId(), hbCon) == null);
//            if (notExist) {
//                conn.setResponseHandler(this);
//                conn.query(sql);
//            }
//        } catch (Exception e) {
//            executeException(conn, e);
//        }
    }

    /**
     * remove timeout connections
     */
    public void abandTimeOuttedConns() {
        if (allCons.isEmpty()) {
            return;
        }
        Collection<NettyBackendSession> abandCons = new LinkedList<NettyBackendSession>();
        long curTime = System.currentTimeMillis();
        Iterator<Map.Entry<Long, HeartBeatCon>> itors = allCons.entrySet()
                .iterator();
        while (itors.hasNext()) {
            HeartBeatCon hbCon = itors.next().getValue();
            if (hbCon.timeOutTimestamp < curTime) {
                abandCons.add(hbCon.conn);
                itors.remove();
            }
        }

        if (!abandCons.isEmpty()) {
            for (NettyBackendSession con : abandCons) {
                try {
                    // if(con.isBorrowed())
//                    con.close("heartbeat timeout ");
                } catch (Exception e) {
                    logger.warn("close err:" + e);
                }
            }
        }

    }


//    @Override
//    public void errorResponse(byte[] data, NettyBackendSession conn) {
//        removeFinished(conn);
//        ErrorPacket err = new ErrorPacket();
//        err.read(data);
//        logger.warn("errorResponse " + err.errno + " "
//                + new String(err.message));
//        conn.release();
//
//    }
//
//    @Override
//    public void okResponse(byte[] ok, NettyBackendSession conn) {
//        boolean executeResponse = conn.syncAndExcute();
//        if (executeResponse) {
//            removeFinished(conn);
//            conn.release();
//        }
//    }

//    private void executeException(NettyBackendSession c, Throwable e) {
//        removeFinished(c);
//        logger.warn("executeException   ", e);
//        c.close("heatbeat exception:" + e);
//    }

    private void removeFinished(NettyBackendSession con) {
//        Long id = ((NettyBackendSession) con).getId();
//        this.allCons.remove(id);
    }

    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {

    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {

    }

    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {

    }

    @Override
    public void send() {

    }
}

class HeartBeatCon {
    public final long timeOutTimestamp;
    public final NettyBackendSession conn;

    public HeartBeatCon(NettyBackendSession conn) {
        super();
        this.timeOutTimestamp = System.currentTimeMillis() + 20 * 1000L;
        this.conn = conn;
    }

}
