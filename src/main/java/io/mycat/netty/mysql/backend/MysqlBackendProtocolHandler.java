package io.mycat.netty.mysql.backend;

import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.packet.ResultSetHeaderPacket;
import io.mycat.netty.mysql.packet.RowDataPacket;
import io.mycat.netty.mysql.proto.Packet;
import io.mycat.netty.mysql.response.ErrorCode;
import io.mycat.netty.mysql.response.ResultSetPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Created by snow_young on 16/8/12.
 */
public class MysqlBackendProtocolHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(MysqlHandshakeHandler.class);

    NettyBackendSession session = null;

    public MysqlBackendProtocolHandler(NettyBackendSession session) {
        this.session = session;
    }

    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext) {
        logger.info("mysql response handler active, ");
    }

    // insert/update/delete
    // select for resultset
    @Override
    public void channelRead(final ChannelHandlerContext channelHandlerContext, Object msg) {
        logger.info("mysql response handler channel read");
        ByteBuf buffer = (ByteBuf) msg;
        byte[] data = new byte[buffer.readableBytes()];
        buffer.readBytes(data);
        byte type = Packet.getType(data);
        logger.info("type: {}, data: {}", type, data);
        switch (type){
            case OkPacket.FIELD_COUNT:
                this.session.setOkPacket(data);
                logger.info("mysql response handler receive OK PACKET");
                break;
            case ErrorPacket.FIELD_COUNT:
                this.session.setErrorPacket(data);
                logger.info("mysql response handler receive error packet");
                break;
            default:
                // select result
                this.session.getResultSetPacket().read(data);
                if(this.session.getResultSetPacket().isFinished()){
                    logger.info("all finished");
                    this.session.setFinished();
                }
        }
        logger.info("finish channel write");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ErrorPacket errorPacket = new ErrorPacket();
        errorPacket.errno = ErrorCode.ER_ERROR_ON_READ;
        errorPacket.message = "backend connection error occurred".getBytes();
        this.session.setErrorPacket(errorPacket);
        ctx.fireExceptionCaught(cause);
    }
    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {
        logger.info("channel inactive");
    }
}


