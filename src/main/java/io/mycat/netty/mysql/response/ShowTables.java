//package io.mycat.netty.mysql.response;
//
//import com.google.common.base.Strings;
//import io.mycat.netty.Session;
//import io.mycat.netty.conf.Configuration;
//import io.mycat.netty.conf.XMLSchemaLoader;
//import io.mycat.netty.mysql.auth.AbstractPrivilege;
//import io.mycat.netty.mysql.auth.PrivilegeFactory;
//import io.mycat.netty.mysql.packet.EOFPacket;
//import io.mycat.netty.mysql.packet.FieldPacket;
//import io.mycat.netty.mysql.packet.ResultSetHeaderPacket;
//import io.mycat.netty.mysql.packet.RowDataPacket;
//import io.mycat.netty.util.StringUtil;
//
//import java.nio.ByteBuffer;
//import java.util.*;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
///**
// * Created by snow_young on 16/8/10.
// */
//public class ShowTables {
//    private static final int FIELD_COUNT = 1;
//    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
//    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
//    private static final EOFPacket eof = new EOFPacket();
//
//    private static final String SCHEMA_KEY = "schemaName";
//    private static final String LIKE_KEY = "like";
//    private static final Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s+(TABLES)(\\s+(FROM)\\s+([a-zA-Z_0-9]+))?(\\s+(LIKE\\s+'(.*)'))?\\s*",Pattern.CASE_INSENSITIVE);
//
//    /**
//     * response method.
//     * @param c
//     */
//    public static void response(Session session,String stmt,int type) {
//        String showSchemal= SchemaUtil.parseShowTableSchema(stmt) ;
//        String cSchema =showSchemal==null? c.getSchema():showSchemal;
//        SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(cSchema);
//        if(schema != null) {
//            //不分库的schema，show tables从后端 mysql中查
//            String node = schema.getDataNode();
//            if(!Strings.isNullOrEmpty(node)) {
//                c.execute(stmt, ServerParse.SHOW);
//                return;
//            }
//        } else {
//            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,"No database selected");
//        }
//
//        //分库的schema，直接从SchemaConfig中获取所有表名
//        Map<String,String> parm = buildFields(c,stmt);
//        java.util.Set<String> tableSet = getTableSet(c, parm);
//
//
//        int i = 0;
//        byte packetId = 0;
//        header.packetId = ++packetId;
//        fields[i] = PacketUtil.getField("Tables in " + parm.get(SCHEMA_KEY), Fields.FIELD_TYPE_VAR_STRING);
//        fields[i++].packetId = ++packetId;
//        eof.packetId = ++packetId;
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
//        // write eof
//        buffer = eof.write(buffer, c,true);
//
//        // write rows
//        packetId = eof.packetId;
//
//        for (String name : tableSet) {
//            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
////            row.add(StringUtil.encode(name.toLowerCase(), c.getCharset()));
//            row.add(StringUtil.encode(name.toLowerCase(), session.getCharset()));
//            row.packetId = ++packetId;
//            buffer = row.write(buffer, c,true);
//        }
//        // write last eof
//        EOFPacket lastEof = new EOFPacket();
//        lastEof.packetId = ++packetId;
//        buffer = lastEof.write(buffer, c,true);
//
//        // post write
//        c.write(buffer);
//
//
//    }
//
//    public static Set<String> getTableSet(Session session, String stmt)
//    {
//        Map<String,String> parm = buildFields(session,stmt);
//        return getTableSet(session, parm);
//
//    }
//
//    private static Set<String> getTableSet(Session session, Map<String, String> parm) {
//        TreeSet<String> tableSet = new TreeSet<String>();
//
//        if (Objects.isNull(parm.get(SCHEMA_KEY))) {
//            if (PrivilegeFactory.getPrivilege().getSchemas(session.getUser()).contains(parm.get(SCHEMA_KEY))) {
//                if (Objects.isNull(parm.get("LIKE_KEY"))) {
//                    tableSet.addAll(Configuration.getSchemaCofnigs().get(parm.get(SCHEMA_KEY)).getTables().keySet());
//                } else {
//                    String p = "^" + parm.get("LIKE_KEY").replace("%", ".*");
//                    Pattern pattern = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
//                    Matcher matcher;
//                    for (String name : Configuration.getSchemaCofnigs().get(parm.get(SCHEMA_KEY)).getTables().keySet()) {
//                        matcher = pattern.matcher(name);
//                        if (matcher.matches()) {
//                            tableSet.add(name);
//                        }
//                    }
//                }
//            }
//        }
//        return tableSet;
//    }
//
//
//
//    /**
//     * build fields
//     * @param session
//     * @param stmt
//     */
//    private static Map<String,String> buildFields(Session session, String stmt) {
//
//        Map<String,String> map = new HashMap<String, String>();
//
//        Matcher ma = pattern.matcher(stmt);
//
//        // should debug and learnging the regex pattern
//        if(ma.find()){
//            String schemaName=ma.group(5);
//            if (null !=schemaName && (!"".equals(schemaName)) && (!"null".equals(schemaName))){
//                map.put(SCHEMA_KEY, schemaName);
//            }
//
//            String like = ma.group(8);
//            if (null !=like && (!"".equals(like)) && (!"null".equals(like))){
//                map.put("LIKE_KEY", like);
//            }
//        }
//
//
//        if(null==map.get(SCHEMA_KEY)){
//            map.put(SCHEMA_KEY, session.getSchema());
//        }
//        return  map;
//    }
//}
