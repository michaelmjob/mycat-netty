package io.mycat.netty.mysql;

import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.SessionService;
import io.mycat.netty.mysql.backend.handler.MultiNodeQueryHandler;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.backend.handler.SingleNodeHandler;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.proto.ERR;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import io.mycat.netty.router.RouteService;
import io.mycat.netty.util.ErrorCode;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by snow_young on 16/8/13.
 * four step :
 *  getSession : push host to routeResultsetNode
 *  send : send by host [host need get session and then send]
 */
@Data
public class MysqlSessionContext {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSessionContext.class);

    // session interface should ultilize
    private MysqlFrontendSession frontSession;
//    private NettyBackendSession backendSession;
//    private ConcurrentHashMap<RouteResultsetNode, NettyBackendSession> target;

    private String sql;
    private RouteResultset rrs;


    public MysqlSessionContext(MysqlFrontendSession frontSession){
        this.frontSession = frontSession;
//        this.target = new ConcurrentHashMap<RouteResultsetNode, NettyBackendSession>(2, 0.75f);
    }

    private void releaseBackendConnections(){
        this.rrs = null;
    }

    public void send2Client(byte[] bytes){
        this.frontSession.writeAndFlush(bytes);
        this.releaseBackendConnections();
    }

    public void send2Client(MySQLPacket mySQLPacket){
        try {
            this.frontSession.writeAndFlush(mySQLPacket.getPacket());
            logger.info("send 2 client");
        }catch (Exception e){
            logger.error("proto parse fail", e);
        }
        this.releaseBackendConnections();
    }

    public void cleanBackendInfo(){
        // responseHandler
        releaseBackendConnections();
//        target.clear();
    }


    public void route(String sql, int type, SchemaConfig schema){
        // 路由计算
        RouteResultset rrs = null;
        try {
            rrs = RouteService.route(type, sql, this);
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
//            send();
        }
    }


    // set Host
    public void getSession(){
        for(RouteResultsetNode node : rrs.getNodes()){
            node.setHost(SessionService.getSession(node.getDataNodeName(), this.frontSession.isAutocommit()));
        }
    }


    public void send(){
        try {
            send0();
        } catch (UnsupportedEncodingException e) {
            logger.error("should not occur this problem : {}", e);
            this.frontSession.sendError(ErrorCode.ER_COLLATION_CHARSET_MISMATCH, "encoding not supported");
        }
    }

    // 通过 rrs 进行发送
    private void send0() throws UnsupportedEncodingException {

        // 检查路由结果是否为空
//        RouteResultsetNode[] nodes = rrs.getNodes();
//        if (nodes == null || nodes.length == 0) {
        if(rrs.size() == 0){
            ErrorPacket errorPacket = new ErrorPacket();
            String msg = "No dataNode found ,please check tables defined in schema:" + getFrontSession().getSchema();
            errorPacket.errno =  ErrorCode.ER_NO_DB_ERROR;
            errorPacket.message = msg.getBytes(this.frontSession.getCharset());
//            this.frontSession.writeAndFlush(errorPacket);
            send2Client(errorPacket);
            // 释放host
            return;
        }

        ResponseHandler responseHandler = null;

//        if (nodes.length == 1) {
        if(rrs.size() == 1){
            responseHandler = new SingleNodeHandler(rrs, this);
        } else {
            boolean autocommit = getFrontSession().isAutocommit();
            responseHandler = new MultiNodeQueryHandler(rrs, autocommit, this);
        }

        for(RouteResultsetNode node : rrs.getNodes()){
            try {
                node.getHost().send(node.getDatabase(), node.getSql(), responseHandler, this);
            } catch (IOException e) {
                logger.error("send sql failed");
                responseHandler.setFinished();
                this.frontSession.sendError(ErrorCode.ER_ERROR_ON_WRITE, "send packet to mysql error");
                logger.error("send sql failed, set finished");
            }
        }

    }

    // 应该把 frontendSession 的功能移植到这边来



    public boolean isClosed() {
        return frontSession.isClosed();
    }
}
