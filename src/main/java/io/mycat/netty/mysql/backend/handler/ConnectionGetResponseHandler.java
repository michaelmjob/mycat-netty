package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/8/14.
 *
 * drop
 */
public class ConnectionGetResponseHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionGetResponseHandler.class);


    public ConnectionGetResponseHandler(){
    }
    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        logger.error("connection handshake failed : {}", packet);
    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
        logger.info("connection success : {}", packet);
    }

    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        logger.info("not come here, {}", resultSetPacket);
    }

    @Override
    public void send() {
        logger.info("should not come here");
    }
}
