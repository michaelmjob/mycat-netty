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
package io.mycat.netty.mysql;

import io.mycat.netty.*;
import io.mycat.netty.mysql.proto.ERR;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.mycat.netty.mysql.proto.Flags;
import io.mycat.netty.mysql.proto.OK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

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
        // refactor : get Session
        MySQLSession session = (MySQLSession) channel.attr(Session.CHANNEL_SESSION_KEY).get();
        assert !Objects.isNull(session);
        if(session == null) {
            throw new IllegalStateException("It is a bug.");
        } else {
            ProtocolTransport transport = new ProtocolTransport(channel, buf);
            userExecutor.execute(new HandleTask(ctx, transport, session));
        }
        
    }
    
    /**
     * Execute the processor in user threads.
     * TODO: JAVA8
     */
    class HandleTask implements Runnable {
        private MySQLSession mysqlSession;

        HandleTask(ChannelHandlerContext ctx, ProtocolTransport transport, MySQLSession session) {
            this.mysqlSession = session;
            this.mysqlSession.setTransport(transport);
            this.mysqlSession.setCtx(ctx);
        }

        public void run() {
            logger.info("processor run");
            try {
                // protocolProcessor 大量使用线程本地变量
                ProtocolProcessor processor = processorFactory.getProcessor(null);
                processor.process(mysqlSession);
            } catch (Throwable e) {
                logger.error("an exception happen when process request", e);
                handleThrowable(e);
            }
        }

        public void handleThrowable(Throwable e) {
            ProtocolProcessException convert = ProtocolProcessException.convert(e);
            ERR err = new ERR();
            err.errorCode = convert.getErrorCode();
            err.errorMessage = convert.getMessage();
            this.mysqlSession.writeAndFlush(err);
        }
    }

//    private void success(Channel channel) {
//        logger.info("success info return form MySQLHandshakeHandler");
//        ByteBuf out = channel.alloc().buffer();
//        OK ok = new OK();
//        ok.sequenceId = 2;
//        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
//        out.writeBytes(ok.toPacket());
//        channel.writeAndFlush(out);
//    }
}
