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

    // whether need to have a packet id, 单节点的packetId的意义不大
    private RouteResultset rrs;
    private RouteResultsetNode node;
    private MysqlSessionContext mysqlSessionContext;

    public SingleNodeHandler(RouteResultset rrs, MysqlSessionContext mysqlSessionContext){
        this.rrs = rrs;
        this.node = rrs.getNodes()[0];
        assert !Objects.isNull(this.node);

        this.mysqlSessionContext = mysqlSessionContext;
    }


    // TODO: refactor, remove session
    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        // context!!
        // TODO: mark log
//        String user = this.mysqlSessionContext.getFrontSession().getUsername();
//        String host = this.mysqlSessionContext.getFrontSession().getHost();
//        int port = this.mysqlSessionContext.getFrontSession().getPort();
//        log.error("execute  sql err : {} , con: {} from frontend: {}/{}/{}", packet.message, user,
//          host, port);
        this.mysqlSessionContext.getFrontSession().writeAndFlush(packet.getPacket());
    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
        // TODO : 需要判断前端连接是否释放掉了！
        // TODO : 判断相应的语句是否属于执行完就释放的
        // TODO : check whether need to reset sequenceId
//        packet.packetId = this.mysqlSessionContext.getFrontSession().getSequenceId();

        this.mysqlSessionContext.getFrontSession().writeAndFlush(packet.getPacket());
//        session.getBackendSession().sendBytes(packet.getPacket());
    }

    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        this.mysqlSessionContext.getFrontSession().writeAndFlush(resultSetPacket.getPacket());
//        session.getBackendSession().sendBytes(resultSetPacket.getPacket());
    }
}
