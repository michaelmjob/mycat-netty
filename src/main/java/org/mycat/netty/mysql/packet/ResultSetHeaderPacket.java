package org.mycat.netty.mysql.packet;

import io.netty.buffer.ByteBuf;
import org.mycat.netty.ProtocolTransport;
import org.mycat.netty.mysql.proto.Flags;
import org.mycat.netty.mysql.proto.Proto;

import java.nio.ByteBuffer;

/**
 *  * From server to client after command, if no error and result set -- that is,
 * if the command was a query which returned a result set. The Result Set Header
 * Packet is the first of several, possibly many, packets that the server sends
 * for result sets. The order of packets for a result set is:
 *
 * <pre>
 * (Result Set Header Packet)   the number of columns
 * (Field Packets)              column descriptors
 * (EOF Packet)                 marker: end of Field Packets
 * (Row Data Packets)           row contents
 * (EOF Packet)                 marker: end of Data Packets
 *
 * Bytes                        Name
 * -----                        ----
 * 1-9   (Length-Coded-Binary)  field_count
 * 1-9   (Length-Coded-Binary)  extra
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Result_Set_Header_Packet
 * </pre>
 *
 * Created by snow_young on 16/8/3.
 */
public class ResultSetHeaderPacket extends MySQLPacket {

    public int fieldCount;
    public long extra;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        this.packetLength = mm.readUB3();
        this.packetId = mm.read();
        this.fieldCount = (int) mm.readLength();
        if (mm.hasRemaining()) {
            this.extra = mm.readLength();
        }
    }

    @Override
    public void write(ByteBuf buf) {
//        int size = calcPacketSize();
//        buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size,writeSocketIfFull);
//        BufferUtil.writeUB3(buffer, size);
//        buffer.put(packetId);
//        BufferUtil.writeLength(buffer, fieldCount);
//        if (extra > 0) {
//            BufferUtil.writeLength(buffer, extra);
//        }
//        return buffer;
        int size = calcPacketSize();
        BufUtil.writeUB3(buf, size);
        buf.writeByte(packetId);
        BufUtil.writeLength(buf, fieldCount);
        if(extra > 0){
            BufUtil.writeLength(buf, extra);
        }
    }

    @Override
    public byte[] getPacket() {
        int size = calcPacketSize();
        byte[] packet = new byte[size + 4];
        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
        int offset = 4;
        byte[] fieldCountBytes = Proto.build_lenenc_int(fieldCount);
        System.arraycopy(fieldCountBytes, 0, packet, offset, fieldCountBytes.length);
        offset += fieldCountBytes.length;

        if(extra > 0){
            byte[] extraBytes = Proto.build_lenenc_int(extra);
            System.arraycopy(extraBytes, 0, packet, offset, extraBytes.length);

        }
        System.out.println("ResultSetHeaderPacket array : " + packet);
        System.out.println("packet ln : " + packet.length + ", expected len: " + size);
        return packet;
    }

    @Override
    public int calcPacketSize() {
        int size = BufferUtil.getLength(fieldCount);
        if (extra > 0) {
            size += BufferUtil.getLength(extra);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL ResultSetHeader Packet";
    }



}
