package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.router.RouteResultset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/8/13.
 */
public class SingleNodeHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeHandler.class);


    public SingleNodeHandler(){

    }
//    public SingleNodeHandler(RouteResultset rrs, )
//

    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
//        session.getBackendSession().sendBytes(packet.getPacket());
    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
//        session.getBackendSession().sendBytes(packet.getPacket());
    }

    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
//        session.getBackendSession().sendBytes(resultSetPacket.getPacket());
    }
}
