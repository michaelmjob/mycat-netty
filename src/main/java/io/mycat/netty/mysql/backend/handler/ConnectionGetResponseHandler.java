package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;

/**
 * Created by snow_young on 16/8/14.
 *
 * drop
 */
public class ConnectionGetResponseHandler implements ResponseHandler{

    private NettyBackendSession session;

    public ConnectionGetResponseHandler(NettyBackendSession session){
        this.session = session;
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
}
