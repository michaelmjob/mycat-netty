package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by snow_young on 16/8/13.
 *
 * 处理收到的结果并准备进行输出：
 *  需要前端的session
 */
public class SingleNodeHandler implements ResponseHandler{
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeHandler.class);


    private RouteResultset rrs;
    private RouteResultsetNode node;
    private MysqlSessionContext mysqlSessionContext;

    public SingleNodeHandler(RouteResultset rrs, MysqlSessionContext mysqlSessionContext){
        this.rrs = rrs;
        this.node = rrs.getNodes()[0];
        assert !Objects.isNull(this.node);

        this.mysqlSessionContext = mysqlSessionContext;
    }


    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        // context!!
        this.mysqlSessionContext.getFrontSession().getChannel().writeAndFlush(packet.getPacket());

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
