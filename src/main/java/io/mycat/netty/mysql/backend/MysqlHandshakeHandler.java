package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.packet.*;
import io.mycat.netty.mysql.proto.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by snow_young on 16/8/12.
 * should test carefully.
 */
// shareable 的实现方式，是单例的意思吗？
public class MysqlHandshakeHandler extends ChannelInboundHandlerAdapter{
    private static final Logger logger = LoggerFactory.getLogger(MysqlHandshakeHandler.class);

    // try attributeKey
    NettyBackendSession session = null;

    public MysqlHandshakeHandler(NettyBackendSession session){
        logger.info("handshake init");
        this.session = session;
    }

    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext){
        logger.info("mysql handshake handler channel active");
        logger.info("channel active count : " +this.session.getCountDownLatch().getCount());
    }

    // 服务端会发送数据过来，需要进行解析。
    @Override
    public void channelRead(final ChannelHandlerContext channelHandlerContext, Object msg){
        logger.info("mysql handshake handler channel read");

        //  这里的协议解析有点问题
        //  channelHandlerContext.
        ByteBuf in = (ByteBuf)msg;
        byte[] packet = new byte[in.readableBytes()];
        in.readBytes(packet);

//        logger.info("packet : {}", packet);
        // 根据类型进行判断
//        logger.info("type : " + Packet.getType(packet));
        HandshakePacket handshakePacket;
        switch(Packet.getType(packet)){
            case OkPacket.FIELD_COUNT:
                // 0x00
                logger.info("authenticate success");
                OkPacket ok = new OkPacket();
                ok.read(packet);
                channelHandlerContext.pipeline().remove(this);
                this.session.getCountDownLatch().countDown();
                session.getResponseHandler().okResponse(ok, session);
                break;
            case ErrorPacket.FIELD_COUNT:
                // 0xff
                ErrorPacket err = new ErrorPacket();
                err.read(packet);
                String errMsg = new String (err.message);
                logger.error("cant't connect to mysql server, errmsg:" + errMsg + " "+this.session);

                // whether need to rmeove
                this.session.getCountDownLatch().countDown();
                session.getResponseHandler().errorResponse(err, session);
//                this.session.getCountDownLatch().countDown();
                break;
            default:
                // begin to uthenticate
                // but here is session!
                assert !Objects.isNull(this.session);
                handshakePacket = this.session.getHandshake();
                if(handshakePacket == null){
                    // receive handshake packet
                    processHandShake(packet);
                    ByteBuf out = Unpooled.buffer(SystemConfig.HANDSHAKE_BUFFER_SIZE);
                    out.writeBytes(session.authenticate());
                    assert !Objects.isNull(this.session);
                    // serverchannel is null

                    // bugfix the serverChannel is abnormal
                    logger.info("wait count : " + this.session.getCountDownLatch().getCount());
                    channelHandlerContext.channel().writeAndFlush(out);

                    break;
                }
                break;
        }
    }

    private void processHandShake(byte[] data){
        HandshakePacket handshake = new HandshakePacket();
        handshake.read(data);

        logger.info("data : {}", data);
        this.session.setHandshake(handshake);
        this.session.setConnectionId(handshake.threadId);

        int charsetIndex = (int) (handshake.serverCharsetIndex & 0xff);
        String charset = io.mycat.netty.mysql.packet.CharsetUtil.getCharset(charsetIndex);
        if(charset != null){
            this.session.setCharsetIndex(charsetIndex);
            this.session.setCharset(charset);
        }else{
            throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("error caught", cause);
        System.exit(-1);
    }

    // need to drop all resource.
    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception{
        logger.info("mysql handshake handler channel read channel inactive");
        super.channelInactive(channelHandlerContext);
    }

}