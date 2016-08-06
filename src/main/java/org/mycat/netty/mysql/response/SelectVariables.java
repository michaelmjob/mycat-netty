package org.mycat.netty.mysql.response;

import com.google.common.base.Splitter;
import org.mycat.netty.mysql.packet.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by snow_young on 16/8/6.
 */
public final class SelectVariables {

    private static final Logger logger = LoggerFactory.getLogger(SelectVariables.class);

    private static final Map<String, String> variables = new HashMap<String, String>();

    static {
        variables.put("@@character_set_client", "utf8");
        variables.put("@@character_set_connection", "utf8");
        variables.put("@@character_set_results", "utf8");
        variables.put("@@character_set_server", "utf8");
        variables.put("@@init_connect", "");
        variables.put("@@interactive_timeout", "172800");
        variables.put("@@license", "GPL");
        variables.put("@@lower_case_table_names", "1");
        variables.put("@@max_allowed_packet", "16777216");
        variables.put("@@net_buffer_length", "16384");
        variables.put("@@net_write_timeout", "60");
        variables.put("@@query_cache_size", "0");
        variables.put("@@query_cache_type", "OFF");
        variables.put("@@sql_mode", "STRICT_TRANS_TABLES");
        variables.put("@@system_time_zone", "CST");
        variables.put("@@time_zone", "SYSTEM");
        variables.put("@@tx_isolation", "REPEATABLE-READ");
        variables.put("@@wait_timeout", "172800");
        variables.put("@@session.auto_increment_increment", "1");

        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("license", "GPL");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "16384");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
        variables.put("auto_increment_increment", "1");
    }


    public static ArrayList<byte[]> getPacket(String sql) {
        ArrayList<byte[]> result = new ArrayList<byte[]>();

        // such as
        // /* mysql-connector-java-5.1.29 ( Revision: alexander.soklakov@oracle.com-20140120140810-s44574olh90i6i4l ) */
        // SELECT @@session.auto_increment_increment
        String subSql = sql.substring(sql.indexOf("SELECT") + 6);
        List<String> splitVar = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql);
        splitVar = convert(splitVar);
        int FIELD_COUNT = splitVar.size();
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++) {
            String s = splitVar.get(i1);
            fields[i] = PacketUtil.getField(s, Fields.FIELD_TYPE_VAR_STRING);
            fields[i++].packetId = ++packetId;
        }

        // write header
        result.add(header.getPacket());

        // write fields
        for (FieldPacket field : fields) {
            result.add(field.getPacket());
        }

        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        // write eof
        result.add(eof.getPacket());

        // write rows
        //byte packetId = eof.packetId;
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++) {
            String s = splitVar.get(i1);
            String value = variables.get(s) == null ? "" : variables.get(s);
            row.add(value.getBytes());
        }

        row.packetId = ++packetId;
        result.add(row.getPacket());

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        result.add(lastEof.getPacket());

        return result;
    }

//    public static void execute(ProtocolTransport transport, String sql) {
//
//        String subSql=   sql.substring(sql.indexOf("SELECT")+6);
//        List<String>  splitVar=   Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql) ;
//        splitVar=convert(splitVar);
//        int FIELD_COUNT = splitVar.size();
//        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
//        FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
//
//        int i = 0;
//        byte packetId = 0;
//        header.packetId = ++packetId;
//        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++)
//        {
//            String s = splitVar.get(i1);
//            fields[i] = PacketUtil.getField(s, Fields.FIELD_TYPE_VAR_STRING);
//            fields[i++].packetId = ++packetId;
//        }
//
//
//        ByteBuffer buffer = c.allocate();
//
//        // write header
//        buffer = header.write(buffer, c,true);
//
//        // write fields
//        for (FieldPacket field : fields) {
//            buffer = field.write(buffer, c,true);
//        }
//
//
//        EOFPacket eof = new EOFPacket();
//        eof.packetId = ++packetId;
//        // write eof
//        buffer = eof.write(buffer, c,true);
//
//        // write rows
//        //byte packetId = eof.packetId;
//
//        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
//        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++)
//        {
//            String s = splitVar.get(i1);
//            String value=  variables.get(s) ==null?"":variables.get(s) ;
//            row.add(value.getBytes());
//
//        }
//
//        row.packetId = ++packetId;
//        buffer = row.write(buffer, c,true);
//
//
//
//        // write lastEof
//        EOFPacket lastEof = new EOFPacket();
//        lastEof.packetId = ++packetId;
//        buffer = lastEof.write(buffer, c,true);
//
//        // write buffer
//        c.write(buffer);
//    }


//    openddal 实现的
//    public static ResultSet getResultSet(String sql) {
//
//        SimpleResultSet result = new SimpleResultSet();
//
//        String subSql = sql.substring(sql.indexOf("SELECT") + 6);
//        List<String> splitVar = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql);
//        splitVar = convert(splitVar);
//        int conut = splitVar.size();
//        for (int i = 0; i < conut; i++) {
//            String s = splitVar.get(i);
//            result.addColumn(s, Types.VARCHAR, Integer.MAX_VALUE, 0);
//        }
//        Object[] row = new Object[conut];
//        for (int i = 0; i < conut; i++) {
//            String s = splitVar.get(i);
//            String value = variables.get(s) == null ? "" : variables.get(s);
//            row[i] = value;
//        }
//        result.addRow(row);
//        return result;
//    }


    private static List<String> convert(List<String> in) {
        logger.info("convert in {}", in);
        List<String> out = new ArrayList(in.size());
        for (String s : in) {
            int asIndex = s.toUpperCase().indexOf(" AS ");
            if (asIndex != -1) {
                out.add(s.substring(asIndex + 4));
            }
        }
        if (out.isEmpty()) {
            return in;
        } else {
            return out;
        }

    }
}
