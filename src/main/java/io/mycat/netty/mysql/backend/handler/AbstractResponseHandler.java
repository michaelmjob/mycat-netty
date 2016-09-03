package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;

import java.nio.ByteBuffer;

/**
 * Created by snow_young on 16/8/29.
 */
public abstract class AbstractResponseHandler implements ResponseHandler {
    protected volatile byte packetId;
    protected volatile ByteBuffer buffer;
    protected volatile boolean isRunning;
    protected Runnable terminateCallBack;
    protected long startTime;
    protected long netInBytes;
    protected long netOutBytes;
    protected long selectRows;
    protected long affectedRows;
    protected boolean finished;

    protected int fieldCount;


    public void send() {

    }

//    @Override
//    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
//        clear(session);
//    }
//
//    @Override
//    public void okResponse(OkPacket packet, NettyBackendSession session) {
//        clear(session);
//    }
//
//    @Override
//    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
//        clear(session);
//    }
//
//    private void clear(NettyBackendSession nettyBackendSession) {
//        nettyBackendSession.back();
//    }

    @Override
    public void setFinished() {
        this.finished = true;
    }
}
