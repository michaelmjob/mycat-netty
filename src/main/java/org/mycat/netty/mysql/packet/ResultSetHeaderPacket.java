package org.mycat.netty.mysql.packet;

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
    public void write(ProtocolTransport transport) {

//        ArrayList<byte[]> payload = new ArrayList<byte[]>();
//        payload.add(Proto.build_byte(Flags.OK));
//        payload.add(Proto.build_lenenc_int(this.affectedRows));
//        payload.add(Proto.build_lenenc_int(this.lastInsertId));
//        payload.add(Proto.build_fixed_int(2, this.statusFlags));
//        payload.add(Proto.build_fixed_int(2, this.warnings));

//        int size = calcPacketSize();
//        BufferUtil.writeUB3(buffer, size);
//        buffer.put(packetId);
//        BufferUtil.writeLength(buffer, fieldCount);
//        if (extra > 0) {
//            BufferUtil.writeLength(buffer, extra);
//        }
//        return buffer;
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
