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

import io.mycat.netty.ProtocolHandler;
import io.mycat.netty.ProtocolTransport;
import io.mycat.netty.Session;
import io.mycat.netty.mysql.auth.Privilege;
import io.mycat.netty.mysql.auth.PrivilegeFactory;
import io.mycat.netty.mysql.packet.AuthPacket;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.HandshakePacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.proto.*;
import io.mycat.netty.util.Constants;
import io.mycat.netty.util.SysProperties;
import io.mycat.netty.util.CharsetUtil;
import io.mycat.netty.util.ErrorCode;
import io.mycat.netty.util.StringUtil;

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
    private final AttributeKey<MysqlFrontendSession> TMP_SESSION_KEY = AttributeKey.valueOf("_AUTHTMP_SESSION_KEY");
    private static final String SEED_KEY = "seed";
    // todo : move to config run
    static{
        PrivilegeFactory.loadPrivilege("");
    }
    private Privilege privilege = PrivilegeFactory.getPrivilege();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("MySQLHandshakeHandler channel Active");

        // maybe leak here
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


        MysqlFrontendSession temp = new MysqlFrontendSession();
        temp.setHandshake(handshake);
        temp.setAttachment(SEED_KEY, handshake.challenge1);
        ctx.attr(TMP_SESSION_KEY).set(temp);

        logger.info("prepare flush authentication, mysql handshake handler : {}", handshake);
        out.writeBytes(handshake.toPacket());
        ctx.writeAndFlush(out);

//  ===========


//        HandshakePacket handshake = new HandshakePacket();
//        handshake.packetId = 0;
//        handshake.protocolVersion = MySQLServer.PROTOCOL_VERSION;
//        // TODO:
//        handshake.serverVersion = MySQLServer.SERVER_VERSION.getBytes();
//        handshake.threadId= connIdGenerator.incrementAndGet();
//        // TODO:
//        handshake.seed = getRandomString(8).getBytes();
//        handshake.serverCapabilities = Flags.CLIENT_BASIC_FLAGS;
//        handshake.serverCharsetIndex = (byte)CharsetUtil.getIndex(MySQLServer.DEFAULT_CHARSET);
//        handshake.serverStatus = Flags.SERVER_STATUS_AUTOCOMMIT;
//        // TODO:
//        handshake.restOfScrambleBuff = getRandomString(12).getBytes();
////        handshake. = 21;
////        handshake.authPluginName = "mysql_native_password";
//        // Remove some flags from the reply
//        handshake.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
//        handshake.removeCapabilityFlag(Flags.CLIENT_IGNORE_SPACE);
//        handshake.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);
//        handshake.removeCapabilityFlag(Flags.CLIENT_SSL);
//        handshake.removeCapabilityFlag(Flags.CLIENT_TRANSACTIONS);
//        handshake.removeCapabilityFlag(Flags.CLIENT_RESERVED);
//        handshake.removeCapabilityFlag(Flags.CLIENT_REMEMBER_OPTIONS);
//
//
//        MysqlFrontendSession temp = new MysqlFrontendSession();
//        temp.setHandshake(handshake);
//        temp.setAttachment(SEED_KEY, handshake.seed);
//        ctx.attr(TMP_SESSION_KEY).set(temp);

//        logger.info("prepare flush authentication, mysql handshake handler : {}", handshake);
//        out.writeBytes(handshake.getPacket());
//        ctx.writeAndFlush(out);
//
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.info("receive authentication, channel read {}", msg);

        // handshake should remove here if authenticate success
        ProtocolTransport transport = new ProtocolTransport(ctx.channel(), (ByteBuf) msg);
        if(transport.getSession() == null) {
            userExecutor.execute(new AuthTask(ctx, transport));
        } else {
            // handshake success, remove self;
            ctx.pipeline().remove(this);
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
     * @return
     */
    private void success(Channel channel) {
        logger.info("success info return form MySQLHandshakeHandler");
        ByteBuf out = channel.alloc().buffer();
        OkPacket okPacket = new OkPacket();
        okPacket.packetId = 2;
        okPacket.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        out.writeBytes(okPacket.getPacket());
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
            MysqlFrontendSession session = ctx.attr(TMP_SESSION_KEY).getAndRemove();
            HandshakeResponse authReply = null;
            AuthPacket authPacket = new AuthPacket();/**/
            try {
                byte[] packet = new byte[transport.in.readableBytes()];
                transport.in.readBytes(packet);
                authPacket.read(packet);


                if (!privilege.userExists(authPacket.user)) {
                    error(ErrorCode.ER_ACCESS_DENIED_ERROR,
                            "Access denied for user '" + authPacket.user + "'");
                    logger.error("user not exist : " + authPacket.user);
                    return;
                }

                if (!StringUtil.isEmpty(authPacket.database)
                        && !privilege.schemaExists(authPacket.user, authPacket.database)) {
                    String s = "Access denied for user '" + authPacket.user
                            + "' to database '" + authPacket.database + "'";
                    logger.error(s);
                    error(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                    return;
                }

                if(!privilege.checkPassword(authPacket.user, authPacket.password, session.getAttachment(SEED_KEY))) {
                    error(ErrorCode.ER_ACCESS_DENIED_ERROR,
                            "Access denied for user '" + authPacket.user + "'");
                    logger.error("wrong name+passwd , name :{}, passwd: {} ", authPacket.user, authPacket.password.toString());
                    return;
                }

                session.setHandshakeResponse(authPacket);
                session.bind(ctx.channel());
                session.setAttachment("remoteAddress", ctx.channel().remoteAddress().toString());
                session.setAttachment("localAddress", ctx.channel().localAddress().toString());
                success(ctx.channel());
            } catch (Exception e) {
                String errMsg = authPacket == null ? e.getMessage()
                        : "Access denied for user '" + authPacket.user + "' to database '" + authPacket.database + "'";
                logger.error("Authorize failed. {},", errMsg, e);
                error(ErrorCode.ER_DBACCESS_DENIED_ERROR, errMsg);
            } finally {
                ctx.writeAndFlush(transport.out);
                transport.in.release();
            }        
        }
        
        public void error(int errno, String msg) {
            logger.info("error msg : " + msg);
            transport.out.clear();
            ErrorPacket errorPacket = new ErrorPacket();
            errorPacket.errno = errno;
            errorPacket.message = msg.getBytes();
            transport.out.writeBytes(errorPacket.getPacket());
        } 
    }
}
