package io.mycat.netty.mysql.backend;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/8/12.
 */
public class MysqlResponseHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(MysqlHandshakeHandler.class);

    public MysqlResponseHandler() {

    }

    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext) {
        logger.info("mysql response handler active, ");
    }

    @Override
    public void channelRead(final ChannelHandlerContext channelHandlerContext, Object msg) {
        logger.info("mysql response handler channel read");

    }

    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {
        logger.info("channel inactive");
    }
}


