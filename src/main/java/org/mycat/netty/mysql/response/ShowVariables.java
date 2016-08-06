package org.mycat.netty.mysql.response;

import org.mycat.netty.ProtocolTransport;
import org.mycat.netty.mysql.packet.*;
import org.mycat.netty.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by snow_young on 16/8/6.
 */
public class ShowVariables {
    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    private static final Map<String, String> variables = new HashMap<String, String>();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("VARIABLE_NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;

        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "8192");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("lower_case_table_names", "1");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
    }



    public static ArrayList<byte[]> getPacket(){
        ArrayList<byte[]> result = new ArrayList<byte[]>();
        // write head
        result.add(header.getPacket());

        // write fields
        for (FieldPacket field : fields) {
            result.add(field.getPacket());
        }

        // write eof
        result.add(eof.getPacket());

        // write rows
        byte packetId = eof.packetId;
        for (Map.Entry<String, String> e : variables.entrySet()) {
            RowDataPacket row = getRow(e.getKey(), e.getValue(), "utf-8");
            row.packetId = ++packetId;
            result.add(row.getPacket());
        }

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        result.add(lastEof.getPacket());

        return result;
    }

    // two implementation
    public static void execute(ProtocolTransport transport) {

        // write header
        header.write(transport.out);
//        transport.getChannel().write(header.getPacket());

        // write fields
        for (FieldPacket field : fields) {
            field.write(transport.out);
//            transport.getChannel().write(field.getPacket());
        }

        // write eof
        eof.write(transport.out);
//        transport.getChannel().write(eof.getPacket());

        // write rows
        byte packetId = eof.packetId;
        for (Map.Entry<String, String> e : variables.entrySet()) {
            RowDataPacket row = getRow(e.getKey(), e.getValue(), "utf-8");
            row.packetId = ++packetId;
            row.write(transport.out);
//            transport.getChannel().write(row.getPacket());
        }

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.write(transport.out);
//        transport.getChannel().write(lastEof.getPacket());

//        transport.getChannel().flush();
    }

    private static RowDataPacket getRow(String name, String value, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(name, charset));
        row.add(StringUtil.encode(value, charset));
        return row;
    }


}
