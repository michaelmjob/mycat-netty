package io.mycat.netty.mysql;

import io.mycat.netty.mysql.backend.SessionService;
import io.mycat.netty.mysql.backend.handler.MultiNodeQueryHandler;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.backend.handler.SingleNodeHandler;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.MySQLPacket;
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

    private String sql;
    private int type;
    private RouteResultset rrs;

    // use status for interceptor easily
    private STATUS currentStatus;

    public MysqlSessionContext(MysqlFrontendSession frontSession){
        this.frontSession = frontSession;
        this.currentStatus = STATUS.INIT;
    }

    private void releaseBackendConnections(){
        this.rrs = null;
    }

    // 处理中心，方便添加inteceptor
    public void process(){
        boolean next = true;
        while(next) {  // 貌似多余了
            switch (currentStatus) {
                case INIT:
                    logger.info("init");

                    currentStatus = STATUS.ROUTE;
                    break;
                case ROUTE:
                    logger.info("route");
                    // String sql, int type, SchemaConfig schema
                    next = route();
                    currentStatus = STATUS.GETSESSION;
                    break;
                case GETSESSION:
                    logger.info("get session");
                    next = getSession();
                    currentStatus = STATUS.SEND2SERVER;
                case SEND2SERVER:
                    logger.info("send 2 server");
                    next  = send();
                    currentStatus = STATUS.RECEIVE;
                    break;
                case RECEIVE:
                    logger.info("receive time");

                    currentStatus = STATUS.SEND2CLIENT;
                    next = false;
                    break;
                case SEND2CLIENT:
//                    logger.info("send 2 client");

                    next = false;
                    currentStatus = STATUS.INIT;
                    break;
                case QUIT:
                    logger.info("quit");
                    break;
            }
        }
    }

    public void sendError(int errorCode, String errorMsg)   {
        ErrorPacket errorPacket = new ErrorPacket();
        errorPacket.errno = errorCode;
        try {
            errorPacket.message = errorMsg.getBytes(getCharset());
        } catch (UnsupportedEncodingException e) {
            logger.error("never happen error : {} for sql : {}", e, sql);
            errorPacket.message = errorMsg.getBytes();
        }
        send2Client(errorPacket);
    }

    public void send2Client(MySQLPacket mySQLPacket){
        try {
            this.frontSession.writeAndFlush(mySQLPacket);
            logger.info("send 2 client finished");
        }catch (Exception e){
            logger.error("proto parse fail", e);
        }
        this.releaseBackendConnections();
    }

    public void cleanBackendInfo(){
        // responseHandler
        releaseBackendConnections();
    }

    public boolean route(){
        // 路由计算
        try {
            rrs = RouteService.route(this);
            if(Objects.isNull(rrs) || rrs.getNodes().size() == 0){
                logger.info("route fail for sql : {}", sql);
                throw new IllegalArgumentException("route failed");
            }
        } catch (Exception e) {
            StringBuilder s = new StringBuilder();
            logger.warn(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            logger.info("error msg : " + msg);

            sendError(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, msg);
            return false;
        }
        return true;
    }


    // set Host
    public boolean getSession(){
        for(RouteResultsetNode node : rrs.getNodes()){
            logger.info("node info: datanodeName {}, database {}, slave?  {} ", node.getDataNodeName(), node.getDatabase(), node.getCanRunSlave());
            node.setHost(SessionService.getSession(node.getDataNodeName(), isAutocommit()));
        }
        return true;
    }


    public boolean send(){
        try {
            send0();
        } catch (UnsupportedEncodingException e) {
            logger.error("should not occur this problem : {}", e);
            sendError(ErrorCode.ER_COLLATION_CHARSET_MISMATCH, "encoding not supported");
        }
        return false;
    }

    // 通过 rrs 进行发送
    private void send0() throws UnsupportedEncodingException {

        // 检查路由结果是否为空
        if(Objects.isNull(rrs) || rrs.size() == 0){
            String msg = "No dataNode found ,please check tables defined in schema:" + getFrontSession().getSchema();
            sendError(ErrorCode.ER_NO_DB_ERROR, msg);
            // 释放host
            return;
        }

        ResponseHandler responseHandler = null;

        if(rrs.size() == 1){
            responseHandler = new SingleNodeHandler(rrs, this);
        } else {
            responseHandler = new MultiNodeQueryHandler(rrs, this);
        }

        for(RouteResultsetNode node : rrs.getNodes()){
            try {
                node.getHost().send(node.getDatabase(), node.getSql(), responseHandler, this);
            } catch (IOException e) {
                logger.error("send sql failed");
                responseHandler.setFinished();
                sendError(ErrorCode.ER_ERROR_ON_WRITE, "send packet to mysql error");
            }
        }

    }


    // controlled in sessionContext for programming friendly
    public String getSchema(){
        return frontSession.getSchema().toUpperCase();
    }

    public String getCharset(){
        return frontSession.getCharset();
    }

    public boolean isAutocommit(){
        return frontSession.isAutocommit();
    }

    // 应该把 frontendSession 的功能移植到这边来
    public boolean isClosed() {
        return frontSession.isClosed();
    }


    private enum STATUS{
        INIT, ROUTE, GETSESSION, SEND2SERVER, RECEIVE, SEND2CLIENT, QUIT,
    }
}
