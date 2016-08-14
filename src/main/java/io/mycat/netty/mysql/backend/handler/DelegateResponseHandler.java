package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by snow_young on 16/8/13.
 */
public class DelegateResponseHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(DelegateResponseHandler.class);

    private final ResponseHandler target;

    public DelegateResponseHandler(ResponseHandler target){
        this.target = target;
    }

    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        target.errorResponse(packet, session);
    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
        target.okResponse(packet, session);
    }

    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        target.resultsetResponse(resultSetPacket, session);
    }
}
