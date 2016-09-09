package io.mycat.netty.mysql.packet;

import io.mycat.netty.mysql.proto.Flags;
import io.mycat.netty.mysql.proto.Proto;
import io.mycat.netty.router.parser.util.ObjectUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Created by snow_young on 16/8/12.
 */
public class OkPacket extends MySQLPacket {
    private static final Logger logger = LoggerFactory.getLogger(MySQLPacket.class);

    public static final byte FIELD_COUNT = 0X00;
    public static final byte[] ok = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};

    public byte fieldCount = FIELD_COUNT;
    public long affectedRows;
    public long insertId;
    public int serverStatus;
    public int warningCount;
    public byte[] message = null;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;

        MySQLMessage mm = new MySQLMessage(bin.data);
        fieldCount = mm.read();

        affectedRows = mm.readLength();
        insertId = mm.readLength();

        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();

        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
    }

    // 这个完全正确
    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = mm.read();
        affectedRows = mm.readLength();
        insertId = mm.readLength();
        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();

        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
    }

    @Override
    public void write(ByteBuf buffer) {
        int size = calcPacketSize();
        BufUtil.writeUB3(buffer, size);
        buffer.writeByte(packetId);

        // should add operation
        buffer.writeByte(fieldCount);
        if (message != null) {
            buffer.writeBytes(message);
        }
    }

    @Override
    public byte[] getPacket() {

        int size = calcPacketSize();
        byte[] packet = new byte[size + 4];

        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
        int offset = 4;


        packet[offset++] = FIELD_COUNT;

        byte[] affectedRowsBytes = Proto.build_lenenc_int(affectedRows);
        System.arraycopy(affectedRowsBytes, 0, packet, offset, affectedRowsBytes.length);
        // last =+ bug
        offset += affectedRowsBytes.length;

        byte[] insertIdBytes = Proto.build_lenenc_int(insertId);
        System.arraycopy(insertIdBytes, 0, packet, offset, insertIdBytes.length);
        offset += insertIdBytes.length;


        System.arraycopy(Proto.build_fixed_int(2, serverStatus), 0, packet, offset, 2);
        offset += 2;


        System.arraycopy(Proto.build_fixed_int(2, warningCount), 0, packet, offset, 2);
        offset += 2;

        if (!Objects.isNull(message)) {
            byte[] len = Proto.build_lenenc_int(message.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(message, 0, packet, offset, message.length);
            offset += message.length;
        }

        logger.info("OKPacket array : {} ", packet);
        logger.info("packet ln : " + packet.length + ", expected len: " + size);
        return packet;

    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += BufferUtil.getLength(affectedRows);
        i += BufferUtil.getLength(insertId);
        i += 4;
        if (message != null) {
            i += BufferUtil.getLength(message);
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }
}
