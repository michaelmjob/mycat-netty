package io.mycat.netty.mysql.backend.handler;

import com.sun.org.apache.xpath.internal.operations.Bool;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by snow_young on 16/8/14.
 * <p>
 * just for connection new created when deal client connection
 */
public class ConnectionGetResponseHandler implements ResponseHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionGetResponseHandler.class);

    private boolean isFinished;

    private Consumer<Boolean> hook;

    public ConnectionGetResponseHandler() {
    }

    public ConnectionGetResponseHandler(Consumer<Boolean> consumer){
        this.hook = consumer;
    }

    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        logger.error("connection handshake failed : {}", packet);
        this.hook.accept(Boolean.FALSE);
    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
        logger.info("connection success : {}", packet);
        this.hook.accept(Boolean.TRUE);
    }

    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        logger.info("not come here, {}", resultSetPacket);
        this.hook.accept(Boolean.FALSE);
    }

    @Override
    public void send() {
        logger.info("should not come here");
    }

    @Override
    public void setFinished() {
        isFinished = true;
    }
}
