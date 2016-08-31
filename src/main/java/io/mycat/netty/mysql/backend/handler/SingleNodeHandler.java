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
public class SingleNodeHandler extends AbstractResponseHandler implements ResponseHandler{
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
        this.mysqlSessionContext.send2Client(packet);

    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
        // TODO : 需要判断前端连接是否释放掉了！
        // TODO : 判断相应的语句是否属于执行完就释放的
        // TODO : check whether need to reset sequenceId
//        packet.packetId = this.mysqlSessionContext.getFrontSession().getSequenceId();

        this.mysqlSessionContext.send2Client(packet);
//        this.mysqlSessionContext.getFrontSession().writeAndFlush(packet.getPacket());
//        session.getBackendSession().sendBytes(packet.getPacket());
    }



    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        this.mysqlSessionContext.send2Client(resultSetPacket);
    }

    @Override
    public void send(){
        startTime=System.currentTimeMillis();

        this.packetId = 0;
//        final BackendConnection conn = session.getTarget(node);
//
//        logger.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
//        node.setRunOnSlave(rrs.getRunOnSlave());	// 实现 master/slave注解
//        logger.debug("node.getRunOnSlave() " + node.getRunOnSlave());
//
//        if (session.tryExistsCon(conn, node)) {
//            _execute(conn);
//        } else {
//            // create new connection
//            MycatConfig conf = MycatServer.getInstance().getConfig();
//
//            PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
//            dn.getConnection(dn.getDatabase(), sc.isAutocommit(), node, this, node);
//        }
    }
}
