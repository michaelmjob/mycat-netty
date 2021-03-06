package io.mycat.netty.mysql.response;

import io.mycat.netty.ProtocolTransport;
import io.mycat.netty.mysql.packet.*;

/**
 * Created by snow_young on 16/8/4.
 */
public class ShowVersion {
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ProtocolTransport transport) {
//        ByteBuf buffer = transport.out;

        // write header
//        header.write(buffer);
        byte[] headerBytes = header.getPacket();
//        transport.getChannel().writeAndFlush(header.getPacket());

        // write fields
        for (FieldPacket field : fields) {
//            field.write(buffer);
            transport.getChannel().writeAndFlush(field.getPacket());
        }

        // write eof
//        eof.write(buffer);
        transport.getChannel().writeAndFlush(eof.getPacket());

        // write rows
        byte packetId = eof.packetId;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(Versions.SERVER_VERSION);
        row.packetId = ++packetId;
//        row.write(buffer);
//        transport.getChannel().writeAndFlush(row.getPacket());

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.write(transport.out);
//        transport.getChannel().writeAndFlush(transport.out);


//        ctx.writeAndFlush(transport.out);
//        transport.in.release();

//        System.out.println("byte[] is : " + buffer.array());
//        try {
//            transport.getOutputStream().write(buffer.array());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        transport.getChannel().flush();
    }
}
