package io.mycat.netty.mysql.packet;

import io.mycat.netty.mysql.proto.Proto;
import io.netty.buffer.ByteBuf;
import io.mycat.netty.mysql.proto.Flags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * From Server To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 *
 * <pre>
 * Bytes                 Name
 * -----                 ----
 * 1                     field_count, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#EOF_Packet
 * </pre>
 * Created by snow_young on 16/8/3.
 */
public class EOFPacket extends MySQLPacket {
    private static Logger logger = LoggerFactory.getLogger(EOFPacket.class);

    public static final byte FIELD_COUNT = (byte) 0xfe;

    public byte fieldCount = FIELD_COUNT;
    public int warningCount;
    public int status = 2;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = mm.read();
        warningCount = mm.readUB2();
        status = mm.readUB2();
    }

    @Override
    public byte[] getPacket() {

        int size = calcPacketSize();
        byte[] packet = new byte[size+4];

        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
        int offset = 4;

//        // main info
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        payload.add(Proto.build_byte(Flags.EOF));
        payload.add(Proto.build_fixed_int(2, this.warningCount));
        payload.add(Proto.build_fixed_int(2, this.status));
//
        for (byte[] field: payload) {
            System.arraycopy(field, 0, packet, offset, field.length);
            offset += field.length;
        }
        logger.info("EofPacket array : {}", packet);
        logger.info("packet ln : " + packet.length + ", expected len: " + size);
        return packet;
    }

    @Override
    public void write(ByteBuf buf){
        int size = calcPacketSize();
        buf.writeBytes(Proto.build_fixed_int(3, size));
        buf.writeBytes(Proto.build_fixed_int(1, packetId));
        buf.writeBytes(Proto.build_byte(Flags.EOF));
        buf.writeBytes(Proto.build_fixed_int(2, this.warningCount));
        buf.writeBytes(Proto.build_fixed_int(2, this.status));
    }

    @Override
    public int calcPacketSize() {
        return 5;// 1+2+2;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL EOF Packet";
    }
}
