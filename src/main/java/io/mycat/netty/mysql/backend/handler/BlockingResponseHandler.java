package io.mycat.netty.mysql.backend.handler;

import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.MySQLPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Created by snow_young on 16/8/30.
 */
public class BlockingResponseHandler extends AbstractResponseHandler implements ResponseHandler {
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeHandler.class);

    private CountDownLatch countDownLatch;

    @Setter
    private Consumer<MySQLPacket> check;

    public BlockingResponseHandler(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
        if (!Objects.isNull(check))
            check.accept(packet);
        this.countDownLatch.countDown();
    }

    @Override
    public void okResponse(OkPacket packet, NettyBackendSession session) {
        if (!Objects.isNull(check))
            check.accept(packet);
        this.countDownLatch.countDown();
    }


    @Override
    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
        if (!Objects.isNull(check))
            check.accept(resultSetPacket);
        this.countDownLatch.countDown();
    }

    @Override
    public void send() {
    }
}
