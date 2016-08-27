package io.mycat.netty.router.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.conf.TableConfig;
import io.mycat.netty.mysql.parser.ServerParse;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by snow_young on 16/8/27.
 */
public class RouterUtil {
    private static final Logger logger = LoggerFactory.getLogger(RouterUtil.class);


    /**
     * 移除执行语句中的数据库名
     *
     * @param stmt		 执行语句
     * @param schema  	数据库名
     * @return 			执行语句
     *
     * @author mycat
     */

    public static String removeSchema(String stmt, String schema) {
        final String upStmt = stmt.toUpperCase();
        final String upSchema = schema.toUpperCase() + ".";
        int strtPos = 0;
        int indx = 0;
        boolean flag = false;
        indx = upStmt.indexOf(upSchema, strtPos);
        if (indx < 0) {
            StringBuilder sb = new StringBuilder("`").append(
                    schema.toUpperCase()).append("`.");
            indx = upStmt.indexOf(sb.toString(), strtPos);
            flag = true;
            if (indx < 0) {
                return stmt;
            }
        }
        StringBuilder sb = new StringBuilder();
        while (indx > 0) {
            sb.append(stmt.substring(strtPos, indx));
            strtPos = indx + upSchema.length();
            if (flag) {
                strtPos += 2;
            }
            indx = upStmt.indexOf(upSchema, strtPos);
        }
        sb.append(stmt.substring(strtPos));
        return sb.toString();
    }

    /**
     * 获取第一个节点作为路由
     *
     * @param rrs		          数据路由集合
     * @param dataNode  	数据库所在节点
     * @param stmt   		执行语句
     * @return 				数据路由集合
     *
     * @author mycat
     */
    public static RouteResultset routeToSingleNode(RouteResultset rrs,
                                                   String dataNode, String stmt) {
        //
        if (dataNode == null) {
            return rrs;
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(dataNode, stmt);//rrs.getStatement()
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);

//        if (rrs.getCanRunInReadDB() != null) {
//            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
//        }
//        if(rrs.getRunOnSlave() != null){
//            nodes[0].setRunOnSlave(rrs.getRunOnSlave());
//        }

        return rrs;
    }



    /**
     * 修复DDL路由
     *
     * @return RouteResultset
     * @author aStoneGod
     */
    public static RouteResultset routeToDDLNode(RouteResultset rrs, int sqlType, String stmt,SchemaConfig schema) throws SQLSyntaxErrorException {
        stmt = getFixedSql(stmt);
        String tablename = "";
        final String upStmt = stmt.toUpperCase();
        if(upStmt.startsWith("CREATE")){
            if (upStmt.contains("CREATE INDEX ")){
                tablename = RouterUtil.getTableName(stmt, RouterUtil.getCreateIndexPos(upStmt, 0));
            }else {
                tablename = RouterUtil.getTableName(stmt, RouterUtil.getCreateTablePos(upStmt, 0));
            }
        }else if(upStmt.startsWith("DROP")){
            if (upStmt.contains("DROP INDEX ")){
                tablename = RouterUtil.getTableName(stmt, RouterUtil.getDropIndexPos(upStmt, 0));
            }else {
                tablename = RouterUtil.getTableName(stmt, RouterUtil.getDropTablePos(upStmt, 0));
            }
        }else if(upStmt.startsWith("ALTER")){
            tablename = RouterUtil.getTableName(stmt, RouterUtil.getAlterTablePos(upStmt, 0));
        }else if (upStmt.startsWith("TRUNCATE")){
            tablename = RouterUtil.getTableName(stmt, RouterUtil.getTruncateTablePos(upStmt, 0));
        }
        tablename = tablename.toUpperCase();

        if (schema.getTables().containsKey(tablename)){
            if(ServerParse.DDL==sqlType){
//                List<String> dataNodes = new ArrayList<>();
//                Map<String, TableConfig> tables = schema.getTables();
//                TableConfig tc;
//                if (tables != null && (tc = tables.get(tablename)) != null) {
//                    dataNodes = tc.getDataNodes();
//                }
//                Iterator<String> iterator1 = dataNodes.iterator();
//                int nodeSize = dataNodes.size();
//                RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSize];
//
//                for(int i=0;i<nodeSize;i++){
//                    String name = iterator1.next();
//                    nodes[i] = new RouteResultsetNode(name, sqlType, stmt);
//                }
//                rrs.setNodes(nodes);
            }
            return rrs;
        }else if(schema.getDataNode()!=null){		//默认节点ddl
            RouteResultsetNode[] nodes = new RouteResultsetNode[1];
            nodes[0] = new RouteResultsetNode(schema.getDataNode(), sqlType, stmt);
            rrs.setNodes(nodes);
            return rrs;
        }
        //both tablename and defaultnode null
        logger.error("table not in schema----"+tablename);
        throw new SQLSyntaxErrorException("op table not in schema----"+tablename);
    }

    /**
     * 处理SQL
     *
     * @param stmt   执行语句
     * @return 		 处理后SQL
     * @author AStoneGod
     */
    public static String getFixedSql(String stmt){
        stmt = stmt.replaceAll("\r\n", " "); //对于\r\n的字符 用 空格处理 rainbow
        return stmt = stmt.trim(); //.toUpperCase();    
    }

    /**
     * 获取table名字
     *
     * @param stmt  	执行语句
     * @param repPos	开始位置和位数
     * @return 表名
     * @author AStoneGod
     */
    public static String getTableName(String stmt, int[] repPos) {
        int startPos = repPos[0];
        int secInd = stmt.indexOf(' ', startPos + 1);
        if (secInd < 0) {
            secInd = stmt.length();
        }
        int thiInd = stmt.indexOf('(',secInd+1);
        if (thiInd < 0) {
            thiInd = stmt.length();
        }
        repPos[1] = secInd;
        String tableName = "";
        if (stmt.toUpperCase().startsWith("DESC")||stmt.toUpperCase().startsWith("DESCRIBE")){
            tableName = stmt.substring(startPos, thiInd).trim();
        }else {
            tableName = stmt.substring(secInd, thiInd).trim();
        }

        //ALTER TABLE
        if (tableName.contains(" ")){
            tableName = tableName.substring(0,tableName.indexOf(" "));
        }
        int ind2 = tableName.indexOf('.');
        if (ind2 > 0) {
            tableName = tableName.substring(ind2 + 1);
        }
        return tableName;
    }


    /**
     * 获取show语句table名字
     *
     * @param stmt	        执行语句
     * @param repPos   开始位置和位数
     * @return 表名
     * @author AStoneGod
     */
    public static String getShowTableName(String stmt, int[] repPos) {
        int startPos = repPos[0];
        int secInd = stmt.indexOf(' ', startPos + 1);
        if (secInd < 0) {
            secInd = stmt.length();
        }

        repPos[1] = secInd;
        String tableName = stmt.substring(startPos, secInd).trim();

        int ind2 = tableName.indexOf('.');
        if (ind2 > 0) {
            tableName = tableName.substring(ind2 + 1);
        }
        return tableName;
    }

    /**
     * 获取语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt     执行语句
     * @param start      开始位置
     * @return int[]	  关键字位置和占位个数
     *
     * @author mycat
     */
    public static int[] getCreateTablePos(String upStmt, int start) {
        String token1 = "CREATE ";
        String token2 = " TABLE ";
        int createInd = upStmt.indexOf(token1, start);
        int tabInd = upStmt.indexOf(token2, start);
        // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
        if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
            return new int[] { tabInd, token2.length() };
        } else {
            return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt
     *            执行语句
     * @param start
     *            开始位置
     * @return int[]关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getCreateIndexPos(String upStmt, int start) {
        String token1 = "CREATE ";
        String token2 = " INDEX ";
        String token3 = " ON ";
        int createInd = upStmt.indexOf(token1, start);
        int idxInd = upStmt.indexOf(token2, start);
        int onInd = upStmt.indexOf(token3, start);
        // 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
        if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
            return new int[] {onInd , token3.length() };
        } else {
            return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取ALTER语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt   执行语句
     * @param start    开始位置
     * @return int[]   关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getAlterTablePos(String upStmt, int start) {
        String token1 = "ALTER ";
        String token2 = " TABLE ";
        int createInd = upStmt.indexOf(token1, start);
        int tabInd = upStmt.indexOf(token2, start);
        // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
        if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
            return new int[] { tabInd, token2.length() };
        } else {
            return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取DROP语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt 	执行语句
     * @param start  	开始位置
     * @return int[]	关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getDropTablePos(String upStmt, int start) {
        //增加 if exists判断
        if(upStmt.contains("EXISTS")){
            String token1 = "IF ";
            String token2 = " EXISTS ";
            int ifInd = upStmt.indexOf(token1, start);
            int tabInd = upStmt.indexOf(token2, start);
            if (ifInd >= 0 && tabInd > 0 && tabInd > ifInd) {
                return new int[] { tabInd, token2.length() };
            } else {
                return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
            }
        }else {
            String token1 = "DROP ";
            String token2 = " TABLE ";
            int createInd = upStmt.indexOf(token1, start);
            int tabInd = upStmt.indexOf(token2, start);

            if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
                return new int[] { tabInd, token2.length() };
            } else {
                return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
            }
        }
    }


    /**
     * 获取DROP语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt
     *            执行语句
     * @param start
     *            开始位置
     * @return int[]关键字位置和占位个数
     * @author aStoneGod
     */

    public static int[] getDropIndexPos(String upStmt, int start) {
        String token1 = "DROP ";
        String token2 = " INDEX ";
        String token3 = " ON ";
        int createInd = upStmt.indexOf(token1, start);
        int idxInd = upStmt.indexOf(token2, start);
        int onInd = upStmt.indexOf(token3, start);
        // 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
        if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
            return new int[] {onInd , token3.length() };
        } else {
            return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取TRUNCATE语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt    执行语句
     * @param start     开始位置
     * @return int[]	关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getTruncateTablePos(String upStmt, int start) {
        String token1 = "TRUNCATE ";
        String token2 = " TABLE ";
        int createInd = upStmt.indexOf(token1, start);
        int tabInd = upStmt.indexOf(token2, start);
        // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
        if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
            return new int[] { tabInd, token2.length() };
        } else {
            return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt   执行语句
     * @param start    开始位置
     * @return int[]   关键字位置和占位个数
     * @author mycat
     */
    public static int[] getSpecPos(String upStmt, int start) {
        String token1 = " FROM ";
        String token2 = " IN ";
        int tabInd1 = upStmt.indexOf(token1, start);
        int tabInd2 = upStmt.indexOf(token2, start);
        if (tabInd1 > 0) {
            if (tabInd2 < 0) {
                return new int[] { tabInd1, token1.length() };
            }
            return (tabInd1 < tabInd2) ? new int[] { tabInd1, token1.length() }
                    : new int[] { tabInd2, token2.length() };
        } else {
            return new int[] { tabInd2, token2.length() };
        }
    }

    /**
     * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
     *
     * @param upStmt   执行sql
     * @param start    开始位置
     * @return int
     * @author mycat
     */
    public static int getSpecEndPos(String upStmt, int start) {
        int tabInd = upStmt.toUpperCase().indexOf(" LIKE ", start);
        if (tabInd < 0) {
            tabInd = upStmt.toUpperCase().indexOf(" WHERE ", start);
        }
        if (tabInd < 0) {
            return upStmt.length();
        }
        return tabInd;
    }

    public static boolean processWithMycatSeq(SchemaConfig schema, int sqlType,
                                              String origSQL, ServerConnection sc) {
        // check if origSQL is with global sequence
        // @micmiu it is just a simple judgement
        //对应本地文件配置方式：insert into table1(id,name) values(next value for MYCATSEQ_GLOBAL,‘test’);
        if (origSQL.indexOf(" MYCATSEQ_") != -1) {
            processSQL(sc,schema,origSQL,sqlType);
            return true;
        }
        return false;
    }

    public static void processSQL(ServerConnection sc,SchemaConfig schema,String sql,int sqlType){
//		int sequenceHandlerType = MycatServer.getInstance().getConfig().getSystem().getSequnceHandlerType();
        SessionSQLPair sessionSQLPair = new SessionSQLPair(sc.getSession2(), schema, sql, sqlType);
//		if(sequenceHandlerType == 3 || sequenceHandlerType == 4){
//			DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(MycatServer
//					.getInstance().getConfig().getSystem().getSequnceHandlerType());
//			String charset = sessionSQLPair.session.getSource().getCharset();
//			String executeSql = null;
//			try {
//				executeSql = sequenceHandler.getExecuteSql(sessionSQLPair.sql,charset == null ? "utf-8":charset);
//			} catch (UnsupportedEncodingException e) {
//				logger.error("UnsupportedEncodingException!");
//			}
//			sessionSQLPair.session.getSource().routeEndExecuteSQL(executeSql, sessionSQLPair.type,sessionSQLPair.schema);
//		} else {
        MycatServer.getInstance().getSequnceProcessor().addNewSql(sessionSQLPair);
//		}
    }

    public static boolean processInsert(SchemaConfig schema, int sqlType,
                                        String origSQL, ServerConnection sc) throws SQLNonTransientException {
        String tableName = StringUtil.getTableName(origSQL).toUpperCase();
        TableConfig tableConfig = schema.getTables().get(tableName);
        boolean processedInsert=false;
        //判断是有自增字段
        if (null != tableConfig && tableConfig.isAutoIncrement()) {
            String primaryKey = tableConfig.getPrimaryKey();
            processedInsert=processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey);
        }
        return processedInsert;
    }

    private static boolean isPKInFields(String origSQL,String primaryKey,int firstLeftBracketIndex,int firstRightBracketIndex){

        if (primaryKey == null) {
            throw new RuntimeException("please make sure the primaryKey's config is not null in schemal.xml");
        }

        boolean isPrimaryKeyInFields = false;
        String upperSQL = origSQL.substring(firstLeftBracketIndex, firstRightBracketIndex + 1).toUpperCase();
        for (int pkOffset = 0, primaryKeyLength = primaryKey.length(), pkStart = 0;;) {
            pkStart = upperSQL.indexOf(primaryKey, pkOffset);
            if (pkStart >= 0 && pkStart < firstRightBracketIndex) {
                char pkSide = upperSQL.charAt(pkStart - 1);
                if (pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == '(') {
                    pkSide = upperSQL.charAt(pkStart + primaryKey.length());
                    isPrimaryKeyInFields = pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == ')';
                }
                if (isPrimaryKeyInFields) {
                    break;
                }
                pkOffset = pkStart + primaryKeyLength;
            } else {
                break;
            }
        }
        return isPrimaryKeyInFields;
    }

    public static boolean processInsert(ServerConnection sc,SchemaConfig schema,
                                        int sqlType,String origSQL,String tableName,String primaryKey) throws SQLNonTransientException {

        int firstLeftBracketIndex = origSQL.indexOf("(");
        int firstRightBracketIndex = origSQL.indexOf(")");
        String upperSql = origSQL.toUpperCase();
        int valuesIndex = upperSql.indexOf("VALUES");
        int selectIndex = upperSql.indexOf("SELECT");
        int fromIndex = upperSql.indexOf("FROM");
        //屏蔽insert into table1 select * from table2语句
        if(firstLeftBracketIndex < 0) {
            String msg = "invalid sql:" + origSQL;
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        //屏蔽批量插入
        if(selectIndex > 0 &&fromIndex>0&&selectIndex>firstRightBracketIndex&&valuesIndex<0) {
            String msg = "multi insert not provided" ;
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        //插入语句必须提供列结构，因为MyCat默认对于表结构无感知
        if(valuesIndex + "VALUES".length() <= firstLeftBracketIndex) {
            throw new SQLSyntaxErrorException("insert must provide ColumnList");
        }
        //如果主键不在插入语句的fields中，则需要进一步处理
        boolean processedInsert=!isPKInFields(origSQL,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);
        if(processedInsert){
            processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey,firstLeftBracketIndex+1,origSQL.indexOf('(',firstRightBracketIndex)+1);
        }
        return processedInsert;
    }

    private static void processInsert(ServerConnection sc, SchemaConfig schema, int sqlType, String origSQL,
                                      String tableName, String primaryKey, int afterFirstLeftBracketIndex, int afterLastLeftBracketIndex) {
        /**
         * 对于主键不在插入语句的fields中的SQL，需要改写。比如hotnews主键为id，插入语句为：
         * insert into hotnews(title) values('aaa');
         * 需要改写成：
         * insert into hotnews(id, title) values(next value for MYCATSEQ_hotnews,'aaa');
         */
        int primaryKeyLength = primaryKey.length();
        int insertSegOffset = afterFirstLeftBracketIndex;
        String mycatSeqPrefix = "next value for MYCATSEQ_";
        int mycatSeqPrefixLength = mycatSeqPrefix.length();
        int tableNameLength = tableName.length();

        char[] newSQLBuf = new char[origSQL.length() + primaryKeyLength + mycatSeqPrefixLength + tableNameLength + 2];
        origSQL.getChars(0, afterFirstLeftBracketIndex, newSQLBuf, 0);
        primaryKey.getChars(0, primaryKeyLength, newSQLBuf, insertSegOffset);
        insertSegOffset += primaryKeyLength;
        newSQLBuf[insertSegOffset] = ',';
        insertSegOffset++;
        origSQL.getChars(afterFirstLeftBracketIndex, afterLastLeftBracketIndex, newSQLBuf, insertSegOffset);
        insertSegOffset += afterLastLeftBracketIndex - afterFirstLeftBracketIndex;
        mycatSeqPrefix.getChars(0, mycatSeqPrefixLength, newSQLBuf, insertSegOffset);
        insertSegOffset += mycatSeqPrefixLength;
        tableName.getChars(0, tableNameLength, newSQLBuf, insertSegOffset);
        insertSegOffset += tableNameLength;
        newSQLBuf[insertSegOffset] = ',';
        insertSegOffset++;
        origSQL.getChars(afterLastLeftBracketIndex, origSQL.length(), newSQLBuf, insertSegOffset);
        processSQL(sc, schema, new String(newSQLBuf), sqlType);
    }

    public static RouteResultset routeToMultiNode(boolean cache,RouteResultset rrs, Collection<String> dataNodes, String stmt) {
        RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
        int i = 0;
        RouteResultsetNode node;
        // 封装每一个datanode
        for (String dataNode : dataNodes) {
            node = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);
            if (rrs.getCanRunInReadDB() != null) {
                node.setCanRunInReadDB(rrs.getCanRunInReadDB());
            }
            if(rrs.getRunOnSlave() != null){
                nodes[0].setRunOnSlave(rrs.getRunOnSlave());
            }
            nodes[i++] = node;
        }
        rrs.setCacheAble(cache);
        rrs.setNodes(nodes);
        return rrs;
    }

    public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> dataNodes,
                                                  String stmt, boolean isGlobalTable) {

        rrs = routeToMultiNode(cache, rrs, dataNodes, stmt);
        rrs.setGlobalTable(isGlobalTable);
        return rrs;
    }

    // desc 的处理, 随机选择一个节点进行处理
    // show tables [from *] 的相关的处理
    // single node 的支持
    public static void routeForTableMeta(RouteResultset rrs,
                                         SchemaConfig schema, String tableName, String sql) {
        String dataNode = null;
        if (isNoSharding(schema,tableName)) {//不分库的直接从schema中获取dataNode
            dataNode = schema.getDataNode();
        } else {
            // ?
            dataNode = getMetaReadDataNode(schema, tableName);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        // readdb slavedb => 一般来说，读写分离 注定了 read db 是slave db
        nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), sql);
        if (rrs.getCanRunInReadDB() != null) {
            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
        }
        if(rrs.getRunOnSlave() != null){
            nodes[0].setRunOnSlave(rrs.getRunOnSlave());
        }
        rrs.setNodes(nodes);
    }

    /**
     * 根据标名随机获取一个节点 => 获取数据库的表的相关数据
     *
     * @param schema     数据库名
     * @param table      表名
     * @return 			  数据节点
     * @author mycat
     */
    private static String getMetaReadDataNode(SchemaConfig schema,
                                              String table) {
        // Table名字被转化为大写的，存储在schema
        table = table.toUpperCase();
        String dataNode = null;
        //
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(table)) != null) {
            dataNode = tc.getRandomDataNode();
        }
        return dataNode;
    }

    /**
     * 根据 ER分片规则获取路由集合
     *
     * @param stmt            执行的语句
     * @param rrs      		     数据路由集合
     * @param tc	      	     表实体
     * @param joinKeyVal      连接属性
     * @return RouteResultset(数据路由集合)	 * 
     * @throws java.sql.SQLNonTransientException
     * @author mycat
     */

    public static RouteResultset routeByERParentKey(ServerConnection sc,SchemaConfig schema,
                                                    int sqlType,String stmt,
                                                    RouteResultset rrs, TableConfig tc, String joinKeyVal)
            throws SQLNonTransientException {

        // only has one parent level and ER parent key is parent
        // table's partition key
        if (tc.isSecondLevel()
                //判断是否为二级子表（父表不再有父表）
                && tc.getParentTC().getPartitionColumn()
                .equals(tc.getParentKey())) { // using
            // parent
            // rule to
            // find
            // datanode
            Set<ColumnRoutePair> parentColVal = new HashSet<ColumnRoutePair>(1);
            ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
            parentColVal.add(pair);
            Set<String> dataNodeSet = ruleCalculate(tc.getParentTC(), parentColVal);
            if (dataNodeSet.isEmpty() || dataNodeSet.size() > 1) {
                throw new SQLNonTransientException(
                        "parent key can't find  valid datanode ,expect 1 but found: "
                                + dataNodeSet.size());
            }
            String dn = dataNodeSet.iterator().next();
            if (logger.isDebugEnabled()) {
                logger.debug("found partion node (using parent partion rule directly) for child table to insert  "
                        + dn + " sql :" + stmt);
            }
            return RouterUtil.routeToSingleNode(rrs, dn, stmt);
        }
        return null;
    }

    /**
     * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
     */
    public static Set<String> ruleByJoinValueCalculate(RouteResultset rrs, TableConfig tc,
                                                       Set<ColumnRoutePair> colRoutePairSet) throws SQLNonTransientException {

        String joinValue = "";

        if(colRoutePairSet.size() > 1) {
            logger.warn("joinKey can't have multi Value");
        } else {
            Iterator<ColumnRoutePair> it = colRoutePairSet.iterator();
            ColumnRoutePair joinCol = it.next();
            joinValue = joinCol.colValue;
        }

        Set<String> retNodeSet = new LinkedHashSet<String>();

        Set<String> nodeSet;
        if (tc.isSecondLevel()
                && tc.getParentTC().getPartitionColumn()
                .equals(tc.getParentKey())) { // using
            // parent
            // rule to
            // find
            // datanode

            nodeSet = ruleCalculate(tc.getParentTC(),colRoutePairSet);
            if (nodeSet.isEmpty()) {
                throw new SQLNonTransientException(
                        "parent key can't find  valid datanode ,expect 1 but found: "
                                + nodeSet.size());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("found partion node (using parent partion rule directly) for child table to insert  "
                        + nodeSet + " sql :" + rrs.getStatement());
            }
            retNodeSet.addAll(nodeSet);

//			for(ColumnRoutePair pair : colRoutePairSet) {
//				nodeSet = ruleCalculate(tc.getParentTC(),colRoutePairSet);
//				if (nodeSet.isEmpty() || nodeSet.size() > 1) {//an exception would be thrown, if sql was executed on more than on sharding
//					throw new SQLNonTransientException(
//							"parent key can't find  valid datanode ,expect 1 but found: "
//									+ nodeSet.size());
//				}
//				String dn = nodeSet.iterator().next();
//				if (logger.isDebugEnabled()) {
//					logger.debug("found partion node (using parent partion rule directly) for child table to insert  "
//							+ dn + " sql :" + rrs.getStatement());
//				}
//				retNodeSet.addAll(nodeSet);
//			}
            return retNodeSet;
        } else {
            retNodeSet.addAll(tc.getParentTC().getDataNodes());
        }

        return retNodeSet;
    }


    /**
     * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
     */
    public static Set<String> ruleCalculate(TableConfig tc,
                                            Set<ColumnRoutePair> colRoutePairSet) {
        Set<String> routeNodeSet = new LinkedHashSet<String>();
        String col = tc.getRule().getColumn();
        RuleConfig rule = tc.getRule();
        AbstractPartitionAlgorithm algorithm = rule.getRuleAlgorithm();
        for (ColumnRoutePair colPair : colRoutePairSet) {
            if (colPair.colValue != null) {
                Integer nodeIndx = algorithm.calculate(colPair.colValue);
                if (nodeIndx == null) {
                    throw new IllegalArgumentException(
                            "can't find datanode for sharding column:" + col
                                    + " val:" + colPair.colValue);
                } else {
                    String dataNode = tc.getDataNodes().get(nodeIndx);
                    routeNodeSet.add(dataNode);
                    colPair.setNodeId(nodeIndx);
                }
            } else if (colPair.rangeValue != null) {
                Integer[] nodeRange = algorithm.calculateRange(
                        String.valueOf(colPair.rangeValue.beginValue),
                        String.valueOf(colPair.rangeValue.endValue));
                if (nodeRange != null) {
                    /**
                     * 不能确认 colPair的 nodeid是否会有其它影响
                     */
                    if (nodeRange.length == 0) {
                        routeNodeSet.addAll(tc.getDataNodes());
                    } else {
                        ArrayList<String> dataNodes = tc.getDataNodes();
                        String dataNode = null;
                        for (Integer nodeId : nodeRange) {
                            dataNode = dataNodes.get(nodeId);
                            routeNodeSet.add(dataNode);
                        }
                    }
                }
            }

        }
        return routeNodeSet;
    }

    /**
     * 多表路由
     * 根据计算单元, 获取所有的table!
     */
    public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx,
                                                   RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect)
            throws SQLNonTransientException {

        List<String> tables = ctx.getTables();

        if(schema.isNoSharding() || (tables.size() >= 1 && isNoSharding(schema,tables.get(0)))) {
            return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
        }

        //只有一个表的
        if(tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schema, ctx, routeUnit, tables.get(0), rrs, isSelect, cachePool);
        }

        Set<String> retNodesSet = new HashSet<String>();
        // 每个表对应的路由映射
        // 可能存在分片导致的路由
        Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();

        // 分库解析信息不为空
        // ???
        Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if(tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //为分库表找路由 ????
            RouterUtil.findRouteWithcConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, ctx.getSql(), cachePool, isSelect);
            if(rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        // 为全局表和单库表找路由
        // 全局表的概念 ?
        // 单库表 找路由？
        // 单库表和全局表 !!
        for(String tableName : tables) {
            TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
            // 这种算是严重异常!
            if(tableConfig == null) {
                String msg = "can't find table define in schema "+ tableName + " schema:" + schema.getName();
                logger.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            if(tablesRouteMap.get(tableName) == null) { //余下的表都是单库表
                //
                tablesRouteMap.put(tableName, new HashSet<String>());
                tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
            }
        }

        boolean isFirstAdd = true;
        for(Map.Entry<String, Set<String>> entry : tablesRouteMap.entrySet()) {
            if(entry.getValue() == null || entry.getValue().size() == 0) {
                throw new SQLNonTransientException("parent key can't find any valid datanode ");
            } else {
                if(isFirstAdd) {
                    retNodesSet.addAll(entry.getValue());
                    isFirstAdd = false;
                } else {
                    retNodesSet.retainAll(entry.getValue());
                    if(retNodesSet.size() == 0) {//两个表的路由无交集
                        String errMsg = "invalid route in sql, multi tables found but datanode has no intersection "
                                + " sql:" + ctx.getSql();
                        logger.warn(errMsg);
                        throw new SQLNonTransientException(errMsg);
                    }
                }
            }
        }

        if(retNodesSet != null && retNodesSet.size() > 0) {
            String tableName = tables.get(0);
            TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
            if(tableConfig.isDistTable()){
                routeToDistTableNode(tableName,schema, rrs, ctx.getSql(), tablesAndConditions, cachePool, isSelect);
                return rrs;
            }

            if(retNodesSet.size() > 1 && isAllGlobalTable(ctx, schema)) {
                // mulit routes ,not cache route result
                if (isSelect) {
                    rrs.setCacheAble(false);
                    routeToSingleNode(rrs, retNodesSet.iterator().next(), ctx.getSql());
                }
                else {//delete 删除全局表的记录
                    routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql(),true);
                }

            } else {
                routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql());
            }

        }
        return rrs;

    }


    /**
     *
     * 单表路由
     */
    public static RouteResultset tryRouteForOneTable(SchemaConfig schema, DruidShardingParseInfo ctx,
                                                     RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs, boolean isSelect,
                                                     LayerCachePool cachePool) throws SQLNonTransientException {

        if (isNoSharding(schema, tableName)) {
            return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
        }

        TableConfig tc = schema.getTables().get(tableName);
        if(tc == null) {
            String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }

//        if(tc.isDistTable()){
//            return routeToDistTableNode(tableName,schema,rrs,ctx.getSql(), routeUnit.getTablesAndConditions(), cachePool,isSelect);
//        }

        if(tc.isGlobalTable()) {//全局表
            if(isSelect) {
                // global select ,not cache route result
                rrs.setCacheAble(false);
                return routeToSingleNode(rrs, tc.getRandomDataNode(),ctx.getSql());
            } else {//insert into 全局表的记录
                return routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql(),true);
            }
        } else {//单表或者分库表
            if (!checkRuleRequired(schema, ctx, routeUnit, tc)) {
                throw new IllegalArgumentException("route rule for table "
                        + tc.getName() + " is required: " + ctx.getSql());

            }
            if(tc.getPartitionColumn() == null && !tc.isSecondLevel()) {//单表且不是childTable
//				return RouterUtil.routeToSingleNode(rrs, tc.getDataNodes().get(0),ctx.getSql());
                return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
            } else {
                //每个表对应的路由映射
                Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
                if(routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
                    RouterUtil.findRouteWithcConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, ctx.getSql(), cachePool, isSelect);
                    if(rrs.isFinishedRoute()) {
                        return rrs;
                    }
                }

                if(tablesRouteMap.get(tableName) == null) {
                    return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
                } else {
                    return routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName), ctx.getSql());
                }
            }
        }
    }

    private static RouteResultset routeToDistTableNode(String tableName, SchemaConfig schema, RouteResultset rrs,
                                                       String orgSql, Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
                                                       boolean isSelect) throws SQLNonTransientException {

        TableConfig tableConfig = schema.getTables().get(tableName);
        if(tableConfig == null) {
            String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }

        String partionCol = tableConfig.getPartitionColumn();
//		String primaryKey = tableConfig.getPrimaryKey();
        boolean isLoadData=false;

        Set<String> tablesRouteSet = new HashSet<String>();

        List<String> dataNodes = tableConfig.getDataNodes();
        if(dataNodes.size()>1){
            String msg = "can't suport district table  " + tableName + " schema:" + schema.getName() + " for mutiple dataNode " + dataNodes;
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        String dataNode = dataNodes.get(0);

        //主键查找缓存暂时不实现
        if(tablesAndConditions.isEmpty()){
            List<String> subTables = tableConfig.getDistTables();
            tablesRouteSet.addAll(subTables);
        }

        for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
            Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();

            Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
            if(partitionValue == null || partitionValue.size() == 0) {
                tablesRouteSet.addAll(tableConfig.getDistTables());
            } else {
                for(ColumnRoutePair pair : partitionValue) {
                    if(pair.colValue != null) {
                        Integer tableIndex = tableConfig.getRule().getRuleAlgorithm().calculate(pair.colValue);
                        if(tableIndex == null) {
                            String msg = "can't find any valid datanode :" + tableConfig.getName()
                                    + " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                            logger.warn(msg);
                            throw new SQLNonTransientException(msg);
                        }
                        String subTable = tableConfig.getDistTables().get(tableIndex);
                        if(subTable != null) {
                            tablesRouteSet.add(subTable);
                        }
                    }
                    if(pair.rangeValue != null) {
                        Integer[] tableIndexs = tableConfig.getRule().getRuleAlgorithm()
                                .calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
                        for(Integer idx : tableIndexs) {
                            String subTable = tableConfig.getDistTables().get(idx);
                            if(subTable != null) {
                                tablesRouteSet.add(subTable);
                            }
                        }
                    }
                }
            }
        }

        Object[] subTables =  tablesRouteSet.toArray();
        RouteResultsetNode[] nodes = new RouteResultsetNode[subTables.length];
        for(int i=0;i<nodes.length;i++){
            String table = String.valueOf(subTables[i]);
            String changeSql = orgSql;
            nodes[i] = new RouteResultsetNode(dataNode, rrs.getSqlType(), changeSql);//rrs.getStatement()
            nodes[i].setSubTableName(String.valueOf(subTables[i]));

            if (rrs.getCanRunInReadDB() != null) {
                nodes[i].setCanRunInReadDB(rrs.getCanRunInReadDB());
            }
            if(rrs.getRunOnSlave() != null){
                nodes[0].setRunOnSlave(rrs.getRunOnSlave());
            }
        }
        rrs.setNodes(nodes);
        rrs.setSubTables(tablesRouteSet);
        rrs.setFinishedRoute(true);

        return rrs;
    }

    /**
     * 处理分库表路由
     */
    public static void findRouteWithcConditionsForTables(SchemaConfig schema, RouteResultset rrs,
                                                         Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
                                                         Map<String, Set<String>> tablesRouteMap, String sql, LayerCachePool cachePool, boolean isSelect)
            throws SQLNonTransientException {

        //为分库表找路由
        for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            String tableName = entry.getKey().toUpperCase();
            TableConfig tableConfig = schema.getTables().get(tableName);
            if(tableConfig == null) {
                String msg = "can't find table define in schema "
                        + tableName + " schema:" + schema.getName();
                logger.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            if(tableConfig.getDistTables()!=null && tableConfig.getDistTables().size()>0){
                routeToDistTableNode(tableName,schema,rrs,sql, tablesAndConditions, cachePool,isSelect);
            }
            //全局表或者不分库的表略过（全局表后面再计算）
            if(tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
                continue;
            } else {//非全局表：分库表、childTable、其他
                Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
                String joinKey = tableConfig.getJoinKey();
                String partionCol = tableConfig.getPartitionColumn();
                String primaryKey = tableConfig.getPrimaryKey();
                boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
                boolean isLoadData=false;
                if (logger.isDebugEnabled()
                        && sql.startsWith(LoadData.loadDataHint)||rrs.isLoadData()) {
                    //由于load data一次会计算很多路由数据，如果输出此日志会极大降低load data的性能
                    isLoadData=true;
                }
                if(entry.getValue().get(primaryKey) != null && entry.getValue().size() == 1&&!isLoadData)
                {//主键查找
                    // try by primary key if found in cache
                    Set<ColumnRoutePair> primaryKeyPairs = entry.getValue().get(primaryKey);
                    if (primaryKeyPairs != null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("try to find cache by primary key ");
                        }
                        String tableKey = schema.getName() + '_' + tableName;
                        boolean allFound = true;
                        for (ColumnRoutePair pair : primaryKeyPairs) {//可能id in(1,2,3)多主键
                            String cacheKey = pair.colValue;
                            String dataNode = (String) cachePool.get(tableKey, cacheKey);
                            if (dataNode == null) {
                                allFound = false;
                                continue;
                            } else {
                                if(tablesRouteMap.get(tableName) == null) {
                                    tablesRouteMap.put(tableName, new HashSet<String>());
                                }
                                tablesRouteMap.get(tableName).add(dataNode);
                                continue;
                            }
                        }
                        if (!allFound) {
                            // need cache primary key ->datanode relation
                            if (isSelect && tableConfig.getPrimaryKey() != null) {
                                rrs.setPrimaryKey(tableKey + '.' + tableConfig.getPrimaryKey());
                            }
                        } else {//主键缓存中找到了就执行循环的下一轮
                            continue;
                        }
                    }
                }
                if (isFoundPartitionValue) {//分库表
                    Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
                    if(partitionValue == null || partitionValue.size() == 0) {
                        if(tablesRouteMap.get(tableName) == null) {
                            tablesRouteMap.put(tableName, new HashSet<String>());
                        }
                        tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
                    } else {
                        for(ColumnRoutePair pair : partitionValue) {
                            if(pair.colValue != null) {
                                Integer nodeIndex = tableConfig.getRule().getRuleAlgorithm().calculate(pair.colValue);
                                if(nodeIndex == null) {
                                    String msg = "can't find any valid datanode :" + tableConfig.getName()
                                            + " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                                    logger.warn(msg);
                                    throw new SQLNonTransientException(msg);
                                }

                                ArrayList<String> dataNodes = tableConfig.getDataNodes();
                                String node;
                                if (nodeIndex >=0 && nodeIndex < dataNodes.size()) {
                                    node = dataNodes.get(nodeIndex);
                                } else {
                                    node = null;
                                    String msg = "Can't find a valid data node for specified node index :"
                                            + tableConfig.getName() + " -> " + tableConfig.getPartitionColumn()
                                            + " -> " + pair.colValue + " -> " + "Index : " + nodeIndex;
                                    logger.warn(msg);
                                    throw new SQLNonTransientException(msg);
                                }
                                if(node != null) {
                                    if(tablesRouteMap.get(tableName) == null) {
                                        tablesRouteMap.put(tableName, new HashSet<String>());
                                    }
                                    tablesRouteMap.get(tableName).add(node);
                                }
                            }
                            if(pair.rangeValue != null) {
                                Integer[] nodeIndexs = tableConfig.getRule().getRuleAlgorithm()
                                        .calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
                                ArrayList<String> dataNodes = tableConfig.getDataNodes();
                                String node;
                                for(Integer idx : nodeIndexs) {
                                    if (idx >= 0 && idx < dataNodes.size()) {
                                        node = dataNodes.get(idx);
                                    } else {
                                        String msg = "Can't find valid data node(s) for some of specified node indexes :"
                                                + tableConfig.getName() + " -> " + tableConfig.getPartitionColumn();
                                        logger.warn(msg);
                                        throw new SQLNonTransientException(msg);
                                    }
                                    if(node != null) {
                                        if(tablesRouteMap.get(tableName) == null) {
                                            tablesRouteMap.put(tableName, new HashSet<String>());
                                        }
                                        tablesRouteMap.get(tableName).add(node);

                                    }
                                }
                            }
                        }
                    }
                } else if(joinKey != null && columnsMap.get(joinKey) != null && columnsMap.get(joinKey).size() != 0) {//childTable  (如果是select 语句的父子表join)之前要找到root table,将childTable移除,只留下root table
                    Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);

                    Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);

                    if (dataNodeSet.isEmpty()) {
                        throw new SQLNonTransientException(
                                "parent key can't find any valid datanode ");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("found partion nodes (using parent partion rule directly) for child table to update  "
                                + Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
                    }
                    if (dataNodeSet.size() > 1) {
                        routeToMultiNode(rrs.isCacheAble(), rrs, dataNodeSet, sql);
                        rrs.setFinishedRoute(true);
                        return;
                    } else {
                        rrs.setCacheAble(true);
                        routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
                        return;
                    }

                } else {
                    //没找到拆分字段，该表的所有节点都路由
                    if(tablesRouteMap.get(tableName) == null) {
                        tablesRouteMap.put(tableName, new HashSet<String>());
                    }
                    tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
                }
            }
        }
    }

    public static boolean isAllGlobalTable(DruidShardingParseInfo ctx, SchemaConfig schema) {
        boolean isAllGlobal = false;
        for(String table : ctx.getTables()) {
            TableConfig tableConfig = schema.getTables().get(table);
            if(tableConfig!=null && tableConfig.isGlobalTable()) {
                isAllGlobal = true;
            } else {
                return false;
            }
        }
        return isAllGlobal;
    }

    /**
     *
     * @param schema
     * @param ctx
     * @param tc
     * @return true表示校验通过，false表示检验不通过
     */
    public static boolean checkRuleRequired(SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, TableConfig tc) {
        if(!tc.isRuleRequired()) {
            return true;
        }
        boolean hasRequiredValue = false;
        String tableName = tc.getName();
        if(routeUnit.getTablesAndConditions().get(tableName) == null || routeUnit.getTablesAndConditions().get(tableName).size() == 0) {
            hasRequiredValue = false;
        } else {
            for(Map.Entry<String, Set<ColumnRoutePair>> condition : routeUnit.getTablesAndConditions().get(tableName).entrySet()) {

                String colName = condition.getKey();
                //条件字段是拆分字段
                if(colName.equals(tc.getPartitionColumn())) {
                    hasRequiredValue = true;
                    break;
                }
            }
        }
        return hasRequiredValue;
    }


    /**
     * 增加判断支持未配置分片的表走默认的dataNode
     * @param schemaConfig
     * @param tableName
     * @return
     */
    public static boolean isNoSharding(SchemaConfig schemaConfig, String tableName) {
        // Table名字被转化为大写的，存储在schema
        tableName = tableName.toUpperCase();
        if (schemaConfig.isNoSharding()) {
            return true;
        }

        if (schemaConfig.getDataNode() != null && !schemaConfig.getTables().containsKey(tableName)) {
            return true;
        }

        return false;
    }

    /**
     * 判断条件是否永真
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysTrue(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        if(Boolean.TRUE.equals(o)) {
            return true;
        }
        return false;
    }

    /**
     * 判断条件是否永假的
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysFalse(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        if(Boolean.FALSE.equals(o)) {
            return true;
        }
        return false;
    }


    public static boolean processERChildTable(final SchemaConfig schema, final String origSQL,
                                              final ServerConnection sc) throws SQLNonTransientException {
        String tableName = StringUtil.getTableName(origSQL).toUpperCase();
        final TableConfig tc = schema.getTables().get(tableName);
        //判断是否为子表，如果不是，只会返回false
        if (null != tc && tc.isChildTable()) {
            final RouteResultset rrs = new RouteResultset(origSQL, ServerParse.INSERT);
            String joinKey = tc.getJoinKey();
            //因为是Insert语句，用MySqlInsertStatement进行parse
            MySqlInsertStatement insertStmt = (MySqlInsertStatement) (new MySqlStatementParser(origSQL)).parseInsert();
            //判断条件完整性，取得解析后语句列中的joinkey列的index
            int joinKeyIndex = getJoinKeyIndex(insertStmt.getColumns(), joinKey);
            if (joinKeyIndex == -1) {
                String inf = "joinKey not provided :" + tc.getJoinKey() + "," + insertStmt;
                logger.warn(inf);
                throw new SQLNonTransientException(inf);
            }
            //子表不支持批量插入
            if (isMultiInsert(insertStmt)) {
                String msg = "ChildTable multi insert not provided";
                logger.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            //取得joinkey的值
            String joinKeyVal = insertStmt.getValues().getValues().get(joinKeyIndex).toString();
            //解决bug #938，当关联字段的值为char类型时，去掉前后"'"
            String realVal = joinKeyVal;
            if (joinKeyVal.startsWith("'") && joinKeyVal.endsWith("'") && joinKeyVal.length() > 2) {
                realVal = joinKeyVal.substring(1, joinKeyVal.length() - 1);
            }

            String sql = insertStmt.toString();

            // try to route by ER parent partion key
            //如果是二级子表（父表不再有父表）,并且分片字段正好是joinkey字段，调用routeByERParentKey
            RouteResultset theRrs = RouterUtil.routeByERParentKey(sc, schema, ServerParse.INSERT, sql, rrs, tc, realVal);
            if (theRrs != null) {
                boolean processedInsert=false;
                //判断是否需要全局序列号
                if ( sc!=null && tc.isAutoIncrement()) {
                    String primaryKey = tc.getPrimaryKey();
                    processedInsert=processInsert(sc,schema,ServerParse.INSERT,sql,tc.getName(),primaryKey);
                }
                if(processedInsert==false){
                    rrs.setFinishedRoute(true);
                    sc.getSession2().execute(rrs, ServerParse.INSERT);
                }
                return true;
            }

            // route by sql query root parent's datanode
            //如果不是二级子表或者分片字段不是joinKey字段结果为空，则启动异步线程去后台分片查询出datanode
            //只要查询出上一级表的parentkey字段的对应值在哪个分片即可
            final String findRootTBSql = tc.getLocateRTableKeySql().toLowerCase() + joinKeyVal;
            if (logger.isDebugEnabled()) {
                logger.debug("find root parent's node sql " + findRootTBSql);
            }

            ListenableFuture<String> listenableFuture = MycatServer.getInstance().
                    getListeningExecutorService().submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler();
                    return fetchHandler.execute(schema.getName(), findRootTBSql, tc.getRootParent().getDataNodes());
                }
            });


            Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                @Override
                public void onSuccess(String result) {
                    //结果为空，证明上一级表中不存在那条记录，失败
                    if (Strings.isNullOrEmpty(result)) {
                        StringBuilder s = new StringBuilder();
                        logger.warn(s.append(sc.getSession2()).append(origSQL).toString() +
                                " err:" + "can't find (root) parent sharding node for sql:" + origSQL);
                        sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "can't find (root) parent sharding node for sql:" + origSQL);
                        return;
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("found partion node for child table to insert " + result + " sql :" + origSQL);
                    }
                    //找到分片，进行插入（和其他的一样，需要判断是否需要全局自增ID）
                    boolean processedInsert=false;
                    if ( sc!=null && tc.isAutoIncrement()) {
                        try {
                            String primaryKey = tc.getPrimaryKey();
                            processedInsert=processInsert(sc,schema,ServerParse.INSERT,origSQL,tc.getName(),primaryKey);
                        } catch (SQLNonTransientException e) {
                            logger.warn("sequence processInsert error,",e);
                            sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR , "sequence processInsert error," + e.getMessage());
                        }
                    }
                    if(processedInsert==false){
                        RouteResultset executeRrs = RouterUtil.routeToSingleNode(rrs, result, origSQL);
                        sc.getSession2().execute(executeRrs, ServerParse.INSERT);
                    }

                }

                @Override
                public void onFailure(Throwable t) {
                    StringBuilder s = new StringBuilder();
                    logger.warn(s.append(sc.getSession2()).append(origSQL).toString() +
                            " err:" + t.getMessage());
                    sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, t.getMessage() + " " + s.toString());
                }
            }, MycatServer.getInstance().
                    getListeningExecutorService());
            return true;
        }
        return false;
    }

    /**
     * 寻找joinKey的索引
     *
     * @param columns
     * @param joinKey
     * @return -1表示没找到，>=0表示找到了
     */
    private static int getJoinKeyIndex(List<SQLExpr> columns, String joinKey) {
        for (int i = 0; i < columns.size(); i++) {
            String col = StringUtil.removeBackquote(columns.get(i).toString()).toUpperCase();
            if (col.equals(joinKey)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 是否为批量插入：insert into ...values (),()...或 insert into ...select.....
     *
     * @param insertStmt
     * @return
     */
    private static boolean isMultiInsert(MySqlInsertStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1)
                || insertStmt.getQuery() != null;
    }

}
