package org.mycat.netty.mysql.packet;

import io.netty.buffer.ByteBuf;
import org.mycat.netty.mysql.proto.Proto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by snow_young on 16/8/4.
 */
public class BinaryPacket extends MySQLPacket {
    private static Logger logger = LoggerFactory.getLogger(BinaryPacket.class);

    public static final byte OK = 1;
    public static final byte ERROR = 2;
    public static final byte HEADER = 3;
    public static final byte FIELD = 4;
    public static final byte FIELD_EOF = 5;
    public static final byte ROW = 6;
    public static final byte PACKET_EOF = 7;

    public byte[] data;

    public void read(InputStream in) throws IOException {
        packetLength = StreamUtil.readUB3(in);
        packetId = StreamUtil.read(in);
        byte[] ab = new byte[packetLength];
        StreamUtil.read(in, ab, 0, ab.length);
        data = ab;
    }

    @Override
    public byte[] getPacket() {
        int size = calcPacketSize();
        byte[] packet = new byte[size+4];

        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);

        int offset = 4;
        System.arraycopy(data, 0, packet, offset, data.length);
        logger.info("BinaryPacket array : {}", packet.clone());
        logger.info("packet ln : " + packet.length + ", expected len: " + size);
        return packet;
//        maybe return is important !
//        BufferUtil.writeUB3(buffer, calcPacketSize());
//        buffer.put(packetId);
//        buffer = c.writeToBuffer(data, buffer);
//        return buffer;

//        self implement
//        int size = calcPacketSize();
//        byte[] packet = new byte[size+4];
//        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
//        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
//        int offset = 4;
//
//        System.arraycopy(data, 0, packet, offset, data.length);
//        offset += data.length;
//        return packet;
    }

    @Override
    public void write(ByteBuf buf) {
        int size = calcPacketSize();
        buf.writeBytes(Proto.build_fixed_int(3, size));
        buf.writeBytes(Proto.build_fixed_int(1, packetId));
        buf.writeBytes(data);
    }

    @Override
    public int calcPacketSize() {
        return data == null ? 0 : data.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Binary Packet";
    }
}
