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

import io.mycat.netty.ProtocolTransport;
import io.mycat.netty.mysql.packet.*;
import io.mycat.netty.mysql.proto.*;
import io.mycat.netty.router.parser.util.ObjectUtil;
import io.mycat.netty.util.CharsetUtil;
import io.mycat.netty.Session;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
@Data
public class MysqlFrontendSession implements Session {
    private static Logger logger = LoggerFactory.getLogger(MysqlFrontendSession.class);

    private Channel channel;
    // maybe error!
    private ChannelHandlerContext ctx;
    private ProtocolTransport transport;

    private Handshake handshake;
//    private HandshakePacket handshakePacket;
//    private HandshakeResponse handshakeResponse;
    private AuthPacket authPacket;

    private Connection engineConnection;
    private Map<String, Object> attachments = new HashMap<String, Object>();
    private String charset;
    private int charsetIndex;
    public String username;
    public String schema;

    private boolean autocommit = true;

    private long sequenceId;

//    private MysqlSessionContext mysqlSessionContext;

    // TODO: add later
    private String sql;

    public boolean isClosed(){
        return !this.channel.isOpen() ||
                !this.channel.isActive() ||
                !this.channel.isWritable();
    }

    public byte[] read(){
        ByteBuf buffer = transport.in;
        byte[] packet = new byte[buffer.readableBytes()];
        buffer.readBytes(packet);
        this.setSequenceId(Packet.getSequenceId(packet));
        return packet;
    }

    public void writeAndFlush(byte[] bytes){
        logger.info("write to client  by front session : {}", bytes);
        this.transport.out.writeBytes(bytes);
        this.ctx.writeAndFlush(this.transport.out);
        this.transport.in.release();
    }


    public void writeAndFlush(ByteBuf bytes){
        this.ctx.writeAndFlush(bytes);
        this.transport.in.release();
    }

    public void writeAndFlush(List<byte[]> bs){
        for (byte[] bt : bs) {
            this.transport.out.writeBytes(bt);
        }
        this.ctx.writeAndFlush(this.transport.out);
        this.transport.in.release();
    }

    public void writeAndFlush(MySQLPacket mySQLPacket){
        writeAndFlush(mySQLPacket.getPacket());
    }

    public void sendError(int errno, String msg) {
        ErrorPacket errorPacket = new ErrorPacket();
        errorPacket.packetId = (byte)getNextSequenceId();
        errorPacket.errno = errno;
        // TODO: ADD CHARSET SUPPORT
        errorPacket.message = msg.getBytes();


        this.transport.out.writeBytes(errorPacket.getPacket());
        this.ctx.writeAndFlush(this.transport.out);
        this.transport.in.release();
//        getProtocolTransport().out.writeBytes(err.toPacket());
    }

    public void sendOk() {

        OkPacket ok = new OkPacket();
        ok.packetId = (byte)getNextSequenceId();
        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        logger.info("send ok  data : {}", ok.getPacket());

        this.transport.out.writeBytes(ok.getPacket());
        this.ctx.writeAndFlush(this.transport.out);
        this.transport.in.release();
    }


    public long getNextSequenceId() {
        return ++this.sequenceId;
    }

    public void setSequenceId(long sequenceId){
        this.sequenceId = sequenceId;
    }

    /**
     * @return the sessionId
     */
//    public long getConnectionId() {
//        return handshake.connectionId;
//    }
//    public long getConnectionId() {
//        return handshakePacket.threadId ;
//    }

    public long getConnectionId() {
        return handshake.connectionId ;
    }

    @SuppressWarnings("unchecked")
    public <T> T setAttachment(String key, T value) {
        T old = (T) attachments.get(key);
        attachments.put(key, value);
        return old;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttachment(String key) {
        T val = (T) attachments.get(key);
        return val;
    }

    /**
     * @return the user
     */
    public String getUser() {
        return this.username;
    }

    /**
     * @return the charset
     */
    public String getCharset() {
        return this.charset;
    }

    /**
     * @param engineConnection the engineConnection to set
     */
    public void setEngineConnection(Connection engineConnection) {
        this.engineConnection = engineConnection;
    }

    /**
     * @param handshake the handshake to set
     */
    public void setHandshake(Handshake handshake) {
        this.handshake = handshake;
        setCharsetIndex((int)handshake.characterSet);
    }

    public void setHandshakeResponse(AuthPacket authPacket) {
        this.authPacket = authPacket;
        this.username = authPacket.user;
        this.schema = authPacket.database;
        this.setCharsetIndex((int)authPacket.charsetIndex);

    }

    public void bind(Channel channel) {
        this.channel = channel;
        Session old = channel.attr(Session.CHANNEL_SESSION_KEY).get();
        if (old != null) {
            throw new IllegalStateException("session is already existing in channel");
        }
        channel.attr(Session.CHANNEL_SESSION_KEY).set(this);
    }

    //  TODO: check resource close
    public void close() {
        attachments.clear();
        if (channel != null && channel.isOpen()) {
            channel.attr(Session.CHANNEL_SESSION_KEY).remove();
            channel.close();
        }
        this.transport.close();
//        context clear resource rightly after send2client
//        if(!Objects.isNull(mysqlSessionContext)){
//            mysqlSessionContext.close();
//        }
        //
    }
    
    public boolean setCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            return setCharset(charset);
        } else {
            return false;
        }
    }

    public boolean setCharset(String charset) {
        if (charset != null) {
            charset = charset.replace("'", "");
        }
        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.charset = charset.equalsIgnoreCase("utf8mb4") ? "utf8" : charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public int getCharsetIndex() {
        return this.charsetIndex;
    }



}
