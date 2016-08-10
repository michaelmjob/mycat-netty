package io.mycat.netty.mysql.response;

import io.mycat.netty.Session;
import io.mycat.netty.mysql.MySQLSession;
import io.mycat.netty.mysql.auth.Privilege;
import io.mycat.netty.mysql.auth.PrivilegeFactory;
import io.mycat.netty.mysql.packet.*;
import org.apache.commons.codec.binary.StringUtils;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by snow_young on 16/8/9.
 */
public class ShowDatabases {
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }


    // params should be sessionContext
    // add charset encoding
    public static ArrayList<byte[]> getPacket(Session sessiion){
        ArrayList<byte[]> result = new ArrayList<>();

        result.add(header.getPacket());
        for(FieldPacket field : fields){
            result.add(field.getPacket());
        }

        result.add(eof.getPacket());

        byte packetId = eof.packetId;

        // 获取当前用户所能够看到的数据库信息
        Collection<String> databases = PrivilegeFactory.getPrivilege().getSchemas(sessiion.getUser());
        for(String database : databases){
            RowDataPacket row= new RowDataPacket(FIELD_COUNT);
            // charset in sessionContext
            row.add(StringUtils.newString(database.getBytes(), sessiion.getCharset()).getBytes());
            row.packetId = ++packetId;
            result.add(row.getPacket());
        }
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        result.add(lastEof.getPacket());
        return result;
    }

}
