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
import io.mycat.netty.mysql.proto.*;
import io.mycat.netty.util.CharsetUtil;
import io.mycat.netty.Session;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
@Data
public class MysqlFrontendSession implements Session {

    private Channel channel;
    // maybe error!
    private ChannelHandlerContext ctx;
    private ProtocolTransport transport;

    private Handshake handshake;
    private HandshakeResponse handshakeResponse;
    private Connection engineConnection;
    private Map<String, Object> attachments = new HashMap<String, Object>();
    private String charset;
    private int charsetIndex;
    public String username;
    public String schema;

    private boolean autocommit = true;

    private long sequenceId;

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

    }

    public void writeAndFlush(ERR err){
       this.transport.out.clear();
        this.writeAndFlush(err.toPacket());
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

    public void sendError(int errno, String msg) {
        ERR err = new ERR();
        err.sequenceId = getNextSequenceId();
        err.errorCode = errno;
        err.errorMessage = msg;

        this.transport.out.writeBytes(err.toPacket());
        this.ctx.writeAndFlush(this.transport.out);
        this.transport.in.release();
//        getProtocolTransport().out.writeBytes(err.toPacket());
    }

    public void sendOk() {
        OK ok = new OK();
        ok.sequenceId = getNextSequenceId();
        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        this.transport.out.writeBytes(ok.toPacket());
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
    public long getConnectionId() {
        return handshake.connectionId;
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
     * @return the schema
     */
    public String getSchema() {
        return this.schema;
//        return handshakeResponse.schema;
    }

    /**
     * @return the engineConnection
     */
    public Connection getEngineConnection() {
        return engineConnection;
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

    /**
     * @param handshakeResponse the handshakeResponse to set
     */
    public void setHandshakeResponse(HandshakeResponse handshakeResponse) {
        this.handshakeResponse = handshakeResponse;
        this.username = handshakeResponse.username;
        this.schema = handshakeResponse.schema;
        this.setCharsetIndex((int)handshakeResponse.characterSet);

    }

    public void bind(Channel channel) {
        this.channel = channel;
        Session old = channel.attr(Session.CHANNEL_SESSION_KEY).get();
        if (old != null) {
            throw new IllegalStateException("session is already existing in channel");
        }
        channel.attr(Session.CHANNEL_SESSION_KEY).set(this);
    }

    public void close() {
        attachments.clear();
        if (channel != null && channel.isOpen()) {
            channel.attr(Session.CHANNEL_SESSION_KEY).remove();
            channel.close();
        }
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
