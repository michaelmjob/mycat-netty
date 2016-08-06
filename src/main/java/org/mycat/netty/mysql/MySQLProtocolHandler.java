/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mycat.netty.mysql;

import org.mycat.netty.*;
import org.mycat.netty.mysql.proto.ERR;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import org.mycat.netty.mysql.proto.Flags;
import org.mycat.netty.mysql.proto.OK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
@Sharable
public class MySQLProtocolHandler extends ProtocolHandler {

    private static final Logger logger = LoggerFactory.getLogger(MySQLProtocolHandler.class);

    private final ProcessorFactory processorFactory = new MySQLProcessorFactory();

    public ProcessorFactory getProcessorFactory() {
        return processorFactory;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        Channel channel = ctx.channel();
        Session session = channel.attr(Session.CHANNEL_SESSION_KEY).get();
        if(session == null) {
            throw new IllegalStateException("It is a bug.");
        } else {
            ProtocolTransport transport = new ProtocolTransport(channel, buf);
            userExecutor.execute(new HandleTask(ctx, transport));
        }
        
    }
    
    /**
     * Execute the processor in user threads.
     * TODO: JAVA8
     */
    class HandleTask implements Runnable {
        private ChannelHandlerContext ctx;
        private ProtocolTransport transport;

        HandleTask(ChannelHandlerContext ctx, ProtocolTransport transport) {
            this.ctx = ctx;
            this.transport = transport;
        }

        public void run() {
            logger.info("processor run");
            try {
                ProtocolProcessor processor = processorFactory.getProcessor(transport);
                processor.process(transport);
            } catch (Throwable e) {
                logger.error("an exception happen when process request", e);
                handleThrowable(e);
//                ctx.writeAndFlush(transport.out);
//                transport.in.release();
            } finally {
                logger.info("finish return processor");
                ctx.writeAndFlush(transport.out);
//                success(transport.getChannel());
//                transport.getChannel().writeAndFlush(transport.out);
                transport.in.release();
            }
        }
        
        public void handleThrowable(Throwable e) {
            transport.out.clear();
            ProtocolProcessException convert = ProtocolProcessException.convert(e);
            ERR err = new ERR();
            err.errorCode = convert.getErrorCode();
            err.errorMessage = convert.getMessage();
            transport.out.writeBytes(err.toPacket());
        }

    }

    private void success(Channel channel) {
        logger.info("success info return form MySQLHandshakeHandler");
        ByteBuf out = channel.alloc().buffer();
        OK ok = new OK();
        ok.sequenceId = 2;
        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        out.writeBytes(ok.toPacket());
        channel.writeAndFlush(out);
    }

}
