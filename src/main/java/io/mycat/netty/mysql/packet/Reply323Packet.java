package io.mycat.netty.mysql.packet;

import io.mycat.netty.mysql.proto.Proto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/8/12.
 */
public class Reply323Packet extends MySQLPacket{
    private static final Logger logger = LoggerFactory.getLogger(Reply323Packet.class);

    public byte[] seed;


    @Override
    public byte[] getPacket() {
        int size = calcPacketSize();
        byte[] packet = new byte[size+4];

        System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
        System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
        int offset = 4;

        // add null_byte
        System.arraycopy(seed, 0, packet, offset, seed.length);
        offset += seed.length;

        packet[offset] = 0;

        logger.info("Reply323Packet array : {}", packet);
        logger.info("packet ln : " + packet.length + ", expected len: " + size);
        return packet;
    }

    @Override
    public int calcPacketSize() {
        return seed == null ? 1 : seed.length + 1;
    }

    @Override
    protected String getPacketInfo() {
        return "MySql Auth323 Packet";
    }
}
