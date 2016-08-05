package org.mycat.netty.mysql.packet;

import io.netty.buffer.ByteBuf;
import org.mycat.netty.mysql.proto.Flags;
import org.mycat.netty.mysql.proto.Proto;
import org.mycat.netty.util.MysqlDefs;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by snow_young on 16/8/3.
 */
public class FieldPacket extends MySQLPacket {
    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    private static final byte[] FILLER = new byte[2];

    public byte[] catalog = DEFAULT_CATALOG;
    public byte[] db;
    public byte[] table;
    public byte[] orgTable;
    public byte[] name;
    public byte[] orgName;
    public int charsetIndex;
    public long length;
    public int type;
    public int flags;
    public byte decimals;
    public byte[] definition;

    /**
     * 把字节数组转变成FieldPacket
     */
    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        this.packetLength = mm.readUB3();
        this.packetId = mm.read();
        readBody(mm);
    }

    /**
     * 把BinaryPacket转变成FieldPacket
     */
    public void read(BinaryPacket bin) {
        this.packetLength = bin.packetLength;
        this.packetId = bin.packetId;
        readBody(new MySQLMessage(bin.data));
    }

    // TODO: add write method, but to where? bytebuffer or channel or out
    @Override
    public byte[] getPacket() {

//        self implement
        int size = calcPacketSize();
        byte[] packet = new byte[size + 4];
        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);

        getBodyPacket(packet, 4);
        System.out.println("Field Packet array : " + packet);
        System.out.println("packet ln : " + packet.length + ", expected len: " + size);
        return packet;
    }

    public void write(ByteBuf buf) {
        int size = calcPacketSize();
        buf.writeBytes(Proto.build_fixed_int(3, size));
        buf.writeBytes(Proto.build_fixed_int(1, packetId));
        writeBodyPacket(buf);
    }


    @Override
    public int calcPacketSize() {
        int size = (catalog == null ? 1 : BufferUtil.getLength(catalog));
        size += (db == null ? 1 : BufferUtil.getLength(db));
        size += (table == null ? 1 : BufferUtil.getLength(table));
        size += (orgTable == null ? 1 : BufferUtil.getLength(orgTable));
        size += (name == null ? 1 : BufferUtil.getLength(name));
        size += (orgName == null ? 1 : BufferUtil.getLength(orgName));
        size += 13;// 1+2+4+1+2+1+2
        if (definition != null) {
            size += BufferUtil.getLength(definition);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Field Packet";
    }

    private void readBody(MySQLMessage mm) {
        this.catalog = mm.readBytesWithLength();
        this.db = mm.readBytesWithLength();
        this.table = mm.readBytesWithLength();
        this.orgTable = mm.readBytesWithLength();
        this.name = mm.readBytesWithLength();
        this.orgName = mm.readBytesWithLength();
        mm.move(1);
        this.charsetIndex = mm.readUB2();
        this.length = mm.readUB4();
        this.type = mm.read() & 0xff;
        this.flags = mm.readUB2();
        this.decimals = mm.read();
        mm.move(FILLER.length);
        if (mm.hasRemaining()) {
            this.definition = mm.readBytesWithLength();
        }
    }

    private void  getBodyPacket(byte[] packet, int offset){
        byte nullVal = 0;

        if (catalog == null) {
            packet[offset++] = nullVal;
        } else {
            byte[] len = Proto.build_lenenc_int(catalog.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(catalog, 0, packet, offset, catalog.length);
            offset += catalog.length;
        }

        if (db == null) {
            packet[offset++] = nullVal;
        } else {
            byte[] len = Proto.build_lenenc_int(db.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(db, 0, packet, offset, db.length);
            offset += db.length;
        }

        if (table == null) {
            packet[offset++] = nullVal;
        } else {
            byte[] len = Proto.build_lenenc_int(table.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(table, 0, packet, offset, table.length);
            offset += table.length;
        }


        if (orgTable == null) {
            packet[offset++] = nullVal;
        } else {
            byte[] len = Proto.build_lenenc_int(orgTable.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(orgTable, 0, packet, offset, orgTable.length);
            offset += orgTable.length;
        }

        if (name == null) {
            packet[offset++] = nullVal;
        } else {
            byte[] len = Proto.build_lenenc_int(name.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(name, 0, packet, offset, name.length);
            offset += name.length;
        }

        if (orgName == null) {
            packet[offset++] = nullVal;
        } else {
            byte[] len = Proto.build_lenenc_int(orgName.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(orgName, 0, packet, offset, orgName.length);
            offset += orgName.length;
        }

        packet[offset++] = (byte) 0x0C;
        System.arraycopy(Proto.build_fixed_int(2, charsetIndex), 0, packet, offset, 2);
        offset += 2;
        System.arraycopy(Proto.build_fixed_int(4, length), 0, packet, offset, 4);
        offset += 4;
        packet[offset++] = (byte)(type & 0xff);
        System.arraycopy(Proto.build_fixed_int(2, flags), 0, packet, offset, 2);
        offset += 2;
        packet[offset++] = decimals;
        packet[offset++] = (byte)(0x00);
        packet[offset++] = (byte)(type & 0x00);
        if(definition != null){

        }
    }

    private void writeBodyPacket(ByteBuf buffer) {
        byte nullVal = 0;
        BufUtil.writeWithLength(buffer, catalog, nullVal);
        BufUtil.writeWithLength(buffer, db, nullVal);
        BufUtil.writeWithLength(buffer, table, nullVal);
        BufUtil.writeWithLength(buffer, orgTable, nullVal);
        BufUtil.writeWithLength(buffer, name, nullVal);
        BufUtil.writeWithLength(buffer, orgName, nullVal);
        buffer.writeByte((byte) 0x0C);
        BufUtil.writeUB2(buffer, charsetIndex);
        BufUtil.writeUB4(buffer, length);
        buffer.writeByte((byte) (type & 0xff));
        BufUtil.writeUB2(buffer, flags);
        buffer.writeByte(decimals);
        buffer.writeByte((byte) 0x00);
        buffer.writeByte((byte) 0x00);
        //buffer.position(buffer.position() + FILLER.length);
        if (definition != null) {
            BufUtil.writeWithLength(buffer, definition);
        }
    }

    public static final void writeWithLength(byte[] packet, int offset, byte[] src, byte nullValue) {
        if (src == null) {
            packet[offset++] = nullValue;
        } else {
            System.arraycopy(src, 0, packet, 0, src.length);
        }
    }
//    private byte[] getBodyPacket() {
//        byte nullVal = 0;
//        BufferUtil.writeWithLength(buffer, catalog, nullVal);
//        BufferUtil.writeWithLength(buffer, db, nullVal);
//        BufferUtil.writeWithLength(buffer, table, nullVal);
//        BufferUtil.writeWithLength(buffer, orgTable, nullVal);
//        BufferUtil.writeWithLength(buffer, name, nullVal);
//        BufferUtil.writeWithLength(buffer, orgName, nullVal);
//        buffer.put((byte) 0x0C);
//        BufferUtil.writeUB2(buffer, charsetIndex);
//        BufferUtil.writeUB4(buffer, length);
//        buffer.put((byte) (type & 0xff));
//        BufferUtil.writeUB2(buffer, flags);
//        buffer.put(decimals);
//        buffer.put((byte)0x00);
//        buffer.put((byte)0x00);
//        //buffer.position(buffer.position() + FILLER.length);
//        if (definition != null) {
//            BufferUtil.writeWithLength(buffer, definition);
//        }
//
//        ArrayList<byte[]> payload = new ArrayList<byte[]>();
//        payload.add(Proto.build_byte(Flags.EOF));
//        payload.add(Proto.build_fixed_int(2, this.warningCount));
//        payload.add(Proto.build_fixed_int(2, this.status));
//    }

}
