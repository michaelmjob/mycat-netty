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

import org.mycat.netty.util.Constants;
import org.mycat.netty.util.SysProperties;
//import com.openddal.jdbc.JdbcDriver;
import org.mycat.netty.ProtocolHandler;
import org.mycat.netty.ProtocolTransport;
import org.mycat.netty.Session;
import org.mycat.netty.mysql.auth.Privilege;
import org.mycat.netty.mysql.auth.PrivilegeDefault;
import org.mycat.netty.mysql.proto.*;
import org.mycat.netty.util.CharsetUtil;
import org.mycat.netty.util.ErrorCode;
import org.mycat.netty.util.StringUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
@Sharable
public class MySQLHandshakeHandler extends ProtocolHandler {
    private static final Logger logger = LoggerFactory.getLogger(MySQLHandshakeHandler.class);
    
    private final AtomicLong connIdGenerator = new AtomicLong(0);
    private final AttributeKey<MySQLSession> TMP_SESSION_KEY = AttributeKey.valueOf("_AUTHTMP_SESSION_KEY");
    private static final String SEED_KEY = "seed";
    private Privilege privilege = PrivilegeDefault.getPrivilege();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("MySQLHandshakeHandler channel Active");
        
        ByteBuf out = ctx.alloc().buffer();
        Handshake handshake = new Handshake();
        handshake.sequenceId = 0;
        handshake.protocolVersion = MySQLServer.PROTOCOL_VERSION;
        handshake.serverVersion = MySQLServer.SERVER_VERSION;
        handshake.connectionId = connIdGenerator.incrementAndGet();
        handshake.challenge1 = getRandomString(8);
        handshake.capabilityFlags = Flags.CLIENT_BASIC_FLAGS;
        handshake.characterSet = CharsetUtil.getIndex(MySQLServer.DEFAULT_CHARSET);
        handshake.statusFlags = Flags.SERVER_STATUS_AUTOCOMMIT;
        handshake.challenge2 = getRandomString(12);
        handshake.authPluginDataLength = 21;
        handshake.authPluginName = "mysql_native_password";
        // Remove some flags from the reply
        handshake.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        handshake.removeCapabilityFlag(Flags.CLIENT_IGNORE_SPACE);
        handshake.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);
        handshake.removeCapabilityFlag(Flags.CLIENT_SSL);
        handshake.removeCapabilityFlag(Flags.CLIENT_TRANSACTIONS);
        handshake.removeCapabilityFlag(Flags.CLIENT_RESERVED);
        handshake.removeCapabilityFlag(Flags.CLIENT_REMEMBER_OPTIONS);


        MySQLSession temp = new MySQLSession();
        temp.setHandshake(handshake);
        temp.setAttachment(SEED_KEY, handshake.challenge1);
        ctx.attr(TMP_SESSION_KEY).set(temp);
        out.writeBytes(handshake.toPacket());
        ctx.writeAndFlush(out);
    
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.info("channel read {}", msg);

        ProtocolTransport transport = new ProtocolTransport(ctx.channel(), (ByteBuf) msg);
        if(transport.getSession() == null) {
            userExecutor.execute(new AuthTask(ctx, transport));
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Session session = ctx.channel().attr(Session.CHANNEL_SESSION_KEY).get();
        if (session != null) {
            logger.info("channel Inactive, session is not null");
//            JdbcUtils.closeSilently(session.getEngineConnection());
        }
    }

    /**
     * @param authReply
     * @return
     * @throws SQLException
     */
    private Connection connectEngine(HandshakeResponse authReply) throws SQLException {
        Properties prop = new Properties();
        prop.setProperty("user", authReply.username);
        prop.setProperty("password", authReply.authResponse);
        String url = Constants.START_URL + SysProperties.ENGINE_CONFIG_LOCATION;
//        Connection connect = JdbcDriver.load().connect(url, prop);
        return null;
    }

    public static String getRandomString(int length) {
        char[] chars = new char[length];
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            chars[i] = (char) random.nextInt(127);
        }
        return String.valueOf(chars);
    }

    /**
     * @param channel
     * @param buf
     * @return
     */
    private void success(Channel channel) {
        logger.info("success info return form MySQLHandshakeHandler");
        ByteBuf out = channel.alloc().buffer();
        OK ok = new OK();
        ok.sequenceId = 2;
        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        out.writeBytes(ok.toPacket());
        channel.writeAndFlush(out);
    }
    
    /**
     * Execute the processor in user threads.
     */
    class AuthTask implements Runnable {
        private ChannelHandlerContext ctx;
        private ProtocolTransport transport;

        AuthTask(ChannelHandlerContext ctx, ProtocolTransport transport) {
            this.ctx = ctx;
            this.transport = transport;
        }

        public void run() {
            // maybe error
            logger.info("auth task is running");
            MySQLSession session = ctx.attr(TMP_SESSION_KEY).getAndRemove();
            HandshakeResponse authReply = null;
            try {
                byte[] packet = new byte[transport.in.readableBytes()];
                transport.in.readBytes(packet);
                authReply = HandshakeResponse.loadFromPacket(packet);
                
                if (!privilege.userExists(authReply.username)) {
                    error(ErrorCode.ER_ACCESS_DENIED_ERROR,
                            "Access denied for user '" + authReply.username + "'");
                    logger.error("user not exist : " + authReply.username);
                    return;
                }
                
                if (!StringUtil.isEmpty(authReply.schema) 
                        && !privilege.schemaExists(authReply.username, authReply.schema)) {
                    String s = "Access denied for user '" + authReply.username
                            + "' to database '" + authReply.schema + "'";
                    logger.error(s);
                    error(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                    return;
                }

                if(!privilege.checkPassword(authReply.username, authReply.authResponse,
                        (String) session.getAttachment(SEED_KEY))) {
                    error(ErrorCode.ER_ACCESS_DENIED_ERROR,
                            "Access denied for user '" + authReply.username + "'");
                    logger.error("wrong name+passwd , name : " + authReply.username + " ;passwd: " + authReply.authResponse);
                    return;
                }
//                Connection connect = connectEngine(authReply);
                session.setHandshakeResponse(authReply);
//                session.setEngineConnection(connect);
                session.bind(ctx.channel());
                session.setAttachment("remoteAddress", ctx.channel().remoteAddress().toString());
                session.setAttachment("localAddress", ctx.channel().localAddress().toString());
                success(ctx.channel());
            } catch (Exception e) {
                String errMsg = authReply == null ? e.getMessage()
                        : "Access denied for user '" + authReply.username + "' to database '" + authReply.schema + "'";
                logger.error("Authorize failed. " + errMsg, e);
                error(ErrorCode.ER_DBACCESS_DENIED_ERROR, errMsg);
            } finally {
                ctx.writeAndFlush(transport.out);
                transport.in.release();
            }        
        }
        
        public void error(int errno, String msg) {
            logger.info("error mesg : " + msg);
            transport.out.clear();
            ERR err = new ERR();
            err.errorCode = errno;
            err.errorMessage = msg;
            transport.out.writeBytes(err.toPacket());
        } 
    }
}
