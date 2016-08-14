package io.mycat.netty.mysql.packet;

import io.mycat.netty.conf.Capabilities;
import io.mycat.netty.mysql.proto.Proto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.attribute.FileTime;

/**
 * Created by snow_young on 16/8/13.
 */
public class AuthPacket extends MySQLPacket{
    private static final Logger logger = LoggerFactory.getLogger(AuthPacket.class);

    private static final byte[] FILLER = new byte[23];

    public long clientFlags;
    public long maxPacketSize;
    public int charsetIndex;
    public byte[] extra;
    public String user;
    public byte[] password;
    public String database;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        clientFlags = mm.readUB4();
        maxPacketSize = mm.readUB4();
        charsetIndex = (mm.read() & 0xff);
        // read extra
        int current = mm.position();
        int len = (int) mm.readLength();
        if (len > 0 && len < FILLER.length) {
            byte[] ab = new byte[len];
            System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
            this.extra = ab;
        }
        mm.position(current + FILLER.length);
        user = mm.readStringWithNull();
        password = mm.readBytesWithLength();
        if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) && mm.hasRemaining()) {
            database = mm.readStringWithNull();
        }
    }

    public void write(OutputStream out) throws IOException {
        StreamUtil.writeUB3(out, calcPacketSize());
        StreamUtil.write(out, packetId);
        StreamUtil.writeUB4(out, clientFlags);
        StreamUtil.writeUB4(out, maxPacketSize);
        StreamUtil.write(out, (byte) charsetIndex);
        out.write(FILLER);
        if (user == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithNull(out, user.getBytes());
        }
        if (password == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithLength(out, password);
        }
        if (database == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithNull(out, database.getBytes());
        }
    }

    @Override
    public byte[] getPacket() {
        int size = calcPacketSize();
        byte[] packet = new byte[size + 4];

        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
        int offset = 4;

        System.arraycopy(Proto.build_fixed_int(4, clientFlags), 0, packet, offset, 4);
        offset += 4;
        System.arraycopy(Proto.build_fixed_int(4, maxPacketSize), 0, packet, offset, 4);
        offset += 4;
        packet[offset++] = (byte)charsetIndex;
        System.arraycopy(FILLER, 0, packet, offset, FILLER.length);
        offset += FILLER.length;

        if(user == null){
            packet[offset++] = 0;
        }else{
            byte[] username = user.getBytes();
            // byte[] + 0 : NULl的写法
            System.arraycopy(username, 0, packet, offset, username.length);
            offset += username.length;
            packet[offset++] = 0;
        }

        if(password == null){
            packet[offset++] = 0;
        }else{
            byte[] len = Proto.build_lenenc_int(password.length);
            System.arraycopy(len, 0, packet, offset, len.length);
            offset += len.length;
            System.arraycopy(password, 0, packet, offset, password.length);
            offset += password.length;
        }

        if(database == null){
            packet[offset++] = 0;
        }else{
            // NULL END type
            byte[] databaseData = database.getBytes();
            System.arraycopy(databaseData, 0, packet, offset, databaseData.length);
            offset += databaseData.length;
            packet[offset++] = 0;
        }
//        logger.info("AuthPacket array : {}", packet);
//        logger.info("packet ln : " + packet.length + ", expected len: " + size);
        return packet;
    }

    @Override
    public int calcPacketSize() {
        int size = 32;// 4+4+1+23;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Authentication Packet";
    }
}
