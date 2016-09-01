package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;

/**
 * Created by snow_young on 16/8/13.
 */
public interface ResponseHandler {

    void errorResponse(ErrorPacket packet, NettyBackendSession session);

    void okResponse(OkPacket packet, NettyBackendSession session);

    void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session);

    void send();

    void setFinished();
}
