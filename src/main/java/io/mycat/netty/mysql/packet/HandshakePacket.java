package io.mycat.netty.mysql.packet;

import io.mycat.netty.mysql.proto.Proto;

import java.nio.ByteBuffer;

/**
 * From server to client during initial handshake.
 * the protocol is different from openddal!
 *
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 1                            protocol_version
 * n (Null-Terminated String)   server_version
 * 4                            thread_id
 * 8                            scramble_buff
 * 1                            (filler) always 0x00
 * 2                            server_capabilities
 * 1                            server_language
 * 2                            server_status
 * 13                           (filler) always 0x00 ...
 * 13                           rest of scramble_buff (4.1)
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Handshake_Initialization_Packet
 * </pre>
 *
 * Created by snow_young on 16/8/13.
 */
public class HandshakePacket extends MySQLPacket {
    private static final byte[] FILLER_13 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    public byte protocolVersion;
    public byte[] serverVersion;
    public long threadId;
    public byte[] seed;
    public int serverCapabilities;
    public byte serverCharsetIndex;
    public int serverStatus;
    public byte[] restOfScrambleBuff;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = mm.readUB2();
        serverCharsetIndex = mm.read();
        serverStatus = mm.readUB2();
        mm.move(13);
        restOfScrambleBuff = mm.readBytesWithNull();
    }

    public void removeCapabilityFlag(long flag) {
        this.serverCapabilities &= ~flag;
    }

    // 有默认支持的协议版本。
    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = mm.readUB2();
        serverCharsetIndex = mm.read();
        serverStatus = mm.readUB2();
        mm.move(13);
        restOfScrambleBuff = mm.readBytesWithNull();
    }


    @Override
    public byte[] getPacket() {
        int size = calcPacketSize();
        byte[] packet = new byte[size+4];

        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
        int offset = 4;

        packet[offset++] = protocolVersion;
        System.arraycopy(serverVersion, 0, packet, offset, serverVersion.length);
        offset += serverVersion.length;
        packet[offset++] = 0;

        // TODO: rename threadId to connId
        System.arraycopy(Proto.build_fixed_int(4, threadId), 0, packet, offset, 4);
        offset += 4;

        System.arraycopy(seed, 0, packet, offset, seed.length);
        offset += seed.length;

        System.arraycopy(Proto.build_fixed_int(2, serverCapabilities), 0, packet, offset, 2);
        offset += 2;
        packet[offset++] = serverCharsetIndex;

        System.arraycopy(Proto.build_fixed_int(2, serverStatus), 0, packet, offset, 2);
        offset += 2;
        System.arraycopy(FILLER_13, 0, packet, offset, 13);
        offset += 13;

        System.arraycopy(restOfScrambleBuff, 0, packet, offset, restOfScrambleBuff.length);

        return packet;
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        size += serverVersion.length;// n
        size += 5;// 1+4
        size += seed.length;// 8
        size += 19;// 1+2+1+2+13
        size += restOfScrambleBuff.length;// 12
        size += 1;// 1
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Handshake Packet";
    }

}
