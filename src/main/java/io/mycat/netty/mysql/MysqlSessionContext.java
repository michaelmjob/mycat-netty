package io.mycat.netty.mysql;

import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.handler.MultiNodeQueryHandler;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.backend.handler.SingleNodeHandler;
import io.mycat.netty.mysql.proto.ERR;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import io.mycat.netty.router.RouteService;
import io.mycat.netty.util.ErrorCode;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.ws.Response;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by snow_young on 16/8/13.
 */
@Data
public class MysqlSessionContext {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSessionContext.class);

    // session interface should ultilize
    private MysqlFrontendSession frontSession;
//    private NettyBackendSession backendSession;
    private ConcurrentHashMap<RouteResultsetNode, NettyBackendSession> target;

    private RouteResultset rrs;

    public MysqlSessionContext(MysqlFrontendSession frontSession){
        this.frontSession = frontSession;
        this.target = new ConcurrentHashMap<RouteResultsetNode, NettyBackendSession>(2, 0.75f);
    }

    public void releaseConnections(){
        for(RouteResultsetNode node : target.keySet()){
            releaseConnection(node);
        }
    }

    public void route(String sql, int type, SchemaConfig schema){
        // 路由计算
        RouteResultset rrs = null;
        try {
            rrs = RouteService.route(type, sql, this);
//                    MycatServer
//                    .getInstance()
//                    .getRouterservice()
//                    .route(MycatServer.getInstance().getConfig().getSystem(),
//                            schema, type, sql, this.charset, this);

        } catch (Exception e) {
            StringBuilder s = new StringBuilder();
            logger.warn(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            logger.info("error msg : " + msg);

            ERR err = new ERR();
            err.errorCode = ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND;
            err.errorMessage = msg;
            this.frontSession.writeAndFlush(err);
            return;
        }
        if (rrs != null) {
            // session执行
            send(rrs, type);
        }
    }


    public void getSession(RouteResultset routeResultSet){
        for(RouteResultsetNode node : routeResultSet.getNodes()){

        }
    }


    // 通过 rrs 进行发送
    public void send(RouteResultset routeResultSet, int type){

        // clear prev execute resources
//        clearHandlesResources();

        // 检查路由结果是否为空
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0) {
            ERR err = new ERR();
            String msg = "No dataNode found ,please check tables defined in schema:" + getFrontSession().getSchema();
            err.errorCode = ErrorCode.ER_NO_DB_ERROR;
            err.errorMessage = msg;
            this.frontSession.writeAndFlush(err);

            this.frontSession.writeAndFlush(err);
            return;
        }

        ResponseHandler responseHandler = null;

        if (nodes.length == 1) {
            responseHandler = new SingleNodeHandler(rrs, this);
            responseHandler.send();

        } else {
            boolean autocommit = getFrontSession().isAutocommit();
            responseHandler = new MultiNodeQueryHandler(type, rrs, autocommit, this);
            responseHandler.send();
        }
    }

    // 应该把 frontendSession 的功能移植到这边来

    public void releaseConnection(RouteResultsetNode node){
        NettyBackendSession session = target.remove(node);
        if(!Objects.isNull(session)){
            // return back connection

        }
    }

    public void closeAndClearResources(){
        for(NettyBackendSession session : target.values()){
            // node.close(reason);
        }
        target.clear();
        // clearHandlesResources();
    }

    public boolean isClosed() {
        return frontSession.isClosed();
    }
}
