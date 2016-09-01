package io.mycat.netty.router.parser.util;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.wall.spi.WallVisitorUtils;

import com.sun.org.apache.xml.internal.utils.NodeConsumer;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.conf.TableConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * Created by snow_young on 16/8/27.
 */
public class RouterUtil {
    private static final Logger logger = LoggerFactory.getLogger(RouterUtil.class);

    /**
     * 移除执行语句中的数据库名
     *
     * @param stmt   执行语句
     * @param schema 数据库名
     * @return 执行语句
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
     * @param rrs      数据路由集合
     * @param dataNode 数据库所在节点
     * @param stmt     执行语句
     * @return 数据路由集合
     * @author mycat
     */
    public static RouteResultset routeToSingleNode(RouteResultset rrs,
                                                   String dataNode, String stmt) {
        if (dataNode == null) {
            return rrs;
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        // TODO:
        nodes[0] = new RouteResultsetNode(dataNode, "databaseName",stmt);
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);

        if (rrs.getCanRunInReadDB() != null) {
            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
        }
        if (rrs.getRunOnSlave() != null) {
            nodes[0].setCanRunSlave(rrs.getRunOnSlave());
        }

        return rrs;
    }


    /**
     * 处理SQL
     *
     * @param stmt 执行语句
     * @return 处理后SQL
     * @author AStoneGod
     */
    public static String getFixedSql(String stmt) {
        stmt = stmt.replaceAll("\r\n", " "); //对于\r\n的字符 用 空格处理 rainbow
        return stmt = stmt.trim(); //.toUpperCase();    
    }

    /**
     * 获取table名字
     *
     * @param stmt   执行语句
     * @param repPos 开始位置和位数
     * @return 表名
     * @author AStoneGod
     */
    public static String getTableName(String stmt, int[] repPos) {
        int startPos = repPos[0];
        int secInd = stmt.indexOf(' ', startPos + 1);
        if (secInd < 0) {
            secInd = stmt.length();
        }
        int thiInd = stmt.indexOf('(', secInd + 1);
        if (thiInd < 0) {
            thiInd = stmt.length();
        }
        repPos[1] = secInd;
        String tableName = "";
        if (stmt.toUpperCase().startsWith("DESC") || stmt.toUpperCase().startsWith("DESCRIBE")) {
            tableName = stmt.substring(startPos, thiInd).trim();
        } else {
            tableName = stmt.substring(secInd, thiInd).trim();
        }

        //ALTER TABLE
        if (tableName.contains(" ")) {
            tableName = tableName.substring(0, tableName.indexOf(" "));
        }
        int ind2 = tableName.indexOf('.');
        if (ind2 > 0) {
            tableName = tableName.substring(ind2 + 1);
        }
        return tableName;
    }


    /**
     * 获取show语句table名字
     * 暂时不支持show语句
     *
     * @param stmt   执行语句
     * @param repPos 开始位置和位数
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
     * @param upStmt 执行语句
     * @param start  开始位置
     * @return int[]      关键字位置和占位个数
     * @author mycat
     */
    public static int[] getCreateTablePos(String upStmt, int start) {
        String token1 = "CREATE ";
        String token2 = " TABLE ";
        int createInd = upStmt.indexOf(token1, start);
        int tabInd = upStmt.indexOf(token2, start);
        // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
        if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
            return new int[]{tabInd, token2.length()};
        } else {
            return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt 执行语句
     * @param start  开始位置
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
            return new int[]{onInd, token3.length()};
        } else {
            return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取ALTER语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt 执行语句
     * @param start  开始位置
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
            return new int[]{tabInd, token2.length()};
        } else {
            return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取DROP语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt 执行语句
     * @param start  开始位置
     * @return int[]    关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getDropTablePos(String upStmt, int start) {
        //增加 if exists判断
        if (upStmt.contains("EXISTS")) {
            String token1 = "IF ";
            String token2 = " EXISTS ";
            int ifInd = upStmt.indexOf(token1, start);
            int tabInd = upStmt.indexOf(token2, start);
            if (ifInd >= 0 && tabInd > 0 && tabInd > ifInd) {
                return new int[]{tabInd, token2.length()};
            } else {
                return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
            }
        } else {
            String token1 = "DROP ";
            String token2 = " TABLE ";
            int createInd = upStmt.indexOf(token1, start);
            int tabInd = upStmt.indexOf(token2, start);

            if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
                return new int[]{tabInd, token2.length()};
            } else {
                return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
            }
        }
    }


    /**
     * 获取DROP语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt 执行语句
     * @param start  开始位置
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
            return new int[]{onInd, token3.length()};
        } else {
            return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取TRUNCATE语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt 执行语句
     * @param start  开始位置
     * @return int[]    关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getTruncateTablePos(String upStmt, int start) {
        String token1 = "TRUNCATE ";
        String token2 = " TABLE ";
        int createInd = upStmt.indexOf(token1, start);
        int tabInd = upStmt.indexOf(token2, start);
        // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
        if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
            return new int[]{tabInd, token2.length()};
        } else {
            return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
        }
    }

    /**
     * 获取语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt 执行语句
     * @param start  开始位置
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
                return new int[]{tabInd1, token1.length()};
            }
            return (tabInd1 < tabInd2) ? new int[]{tabInd1, token1.length()}
                    : new int[]{tabInd2, token2.length()};
        } else {
            return new int[]{tabInd2, token2.length()};
        }
    }

    /**
     * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
     *
     * @param upStmt 执行sql
     * @param start  开始位置
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


    private static boolean isPKInFields(String origSQL, String primaryKey, int firstLeftBracketIndex, int firstRightBracketIndex) {

        if (primaryKey == null) {
            throw new RuntimeException("please make sure the primaryKey's config is not null in schemal.xml");
        }

        boolean isPrimaryKeyInFields = false;
        String upperSQL = origSQL.substring(firstLeftBracketIndex, firstRightBracketIndex + 1).toUpperCase();
        for (int pkOffset = 0, primaryKeyLength = primaryKey.length(), pkStart = 0; ; ) {
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

//    public static boolean processInsert(ServerConnection sc, SchemaConfig schema,
//                                        int sqlType, String origSQL, String tableName, String primaryKey) throws SQLNonTransientException {
//
//        int firstLeftBracketIndex = origSQL.indexOf("(");
//        int firstRightBracketIndex = origSQL.indexOf(")");
//        String upperSql = origSQL.toUpperCase();
//        int valuesIndex = upperSql.indexOf("VALUES");
//        int selectIndex = upperSql.indexOf("SELECT");
//        int fromIndex = upperSql.indexOf("FROM");
//        //屏蔽insert into table1 select * from table2语句
//        if (firstLeftBracketIndex < 0) {
//            String msg = "invalid sql:" + origSQL;
//            logger.warn(msg);
//            throw new SQLNonTransientException(msg);
//        }
//        //屏蔽批量插入
//        if (selectIndex > 0 && fromIndex > 0 && selectIndex > firstRightBracketIndex && valuesIndex < 0) {
//            String msg = "multi insert not provided";
//            logger.warn(msg);
//            throw new SQLNonTransientException(msg);
//        }
//        //插入语句必须提供列结构，因为MyCat默认对于表结构无感知
//        if (valuesIndex + "VALUES".length() <= firstLeftBracketIndex) {
//            throw new SQLSyntaxErrorException("insert must provide ColumnList");
//        }
//        //如果主键不在插入语句的fields中，则需要进一步处理
//        boolean processedInsert = !isPKInFields(origSQL, primaryKey, firstLeftBracketIndex, firstRightBracketIndex);
//        if (processedInsert) {
//            processInsert(sc, schema, sqlType, origSQL, tableName, primaryKey, firstLeftBracketIndex + 1, origSQL.indexOf('(', firstRightBracketIndex) + 1);
//        }
//        return processedInsert;
//    }

//    private static void processInsert(ServerConnection sc, SchemaConfig schema, int sqlType, String origSQL,
//                                      String tableName, String primaryKey, int afterFirstLeftBracketIndex, int afterLastLeftBracketIndex) {
//        /**
//         * 对于主键不在插入语句的fields中的SQL，需要改写。比如hotnews主键为id，插入语句为：
//         * insert into hotnews(title) values('aaa');
//         * 需要改写成：
//         * insert into hotnews(id, title) values(next value for MYCATSEQ_hotnews,'aaa');
//         */
//        int primaryKeyLength = primaryKey.length();
//        int insertSegOffset = afterFirstLeftBracketIndex;
//        String mycatSeqPrefix = "next value for MYCATSEQ_";
//        int mycatSeqPrefixLength = mycatSeqPrefix.length();
//        int tableNameLength = tableName.length();
//
//        char[] newSQLBuf = new char[origSQL.length() + primaryKeyLength + mycatSeqPrefixLength + tableNameLength + 2];
//        origSQL.getChars(0, afterFirstLeftBracketIndex, newSQLBuf, 0);
//        primaryKey.getChars(0, primaryKeyLength, newSQLBuf, insertSegOffset);
//        insertSegOffset += primaryKeyLength;
//        newSQLBuf[insertSegOffset] = ',';
//        insertSegOffset++;
//        origSQL.getChars(afterFirstLeftBracketIndex, afterLastLeftBracketIndex, newSQLBuf, insertSegOffset);
//        insertSegOffset += afterLastLeftBracketIndex - afterFirstLeftBracketIndex;
//        mycatSeqPrefix.getChars(0, mycatSeqPrefixLength, newSQLBuf, insertSegOffset);
//        insertSegOffset += mycatSeqPrefixLength;
//        tableName.getChars(0, tableNameLength, newSQLBuf, insertSegOffset);
//        insertSegOffset += tableNameLength;
//        newSQLBuf[insertSegOffset] = ',';
//        insertSegOffset++;
//        origSQL.getChars(afterLastLeftBracketIndex, origSQL.length(), newSQLBuf, insertSegOffset);
//        processSQL(sc, schema, new String(newSQLBuf), sqlType);
//    }

    //    public static RouteResultset routeToMultiNode(RouteResultset rrs, Collection<String> dataNodes, String stmt) {
    public static RouteResultset routeToMultiNode(RouteResultset rrs, Collection<TableConfig.NodeConfig> dataNodes, String stmt) {
        RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
        int i = 0;
        RouteResultsetNode node;
        // 封装每一个datanode
        for (TableConfig.NodeConfig dataNode : dataNodes) {
            // TODO: ensure the database name
            node = new RouteResultsetNode(dataNode.getDatanode(), dataNode.getDatabase(),stmt);
//            read  write  slave master 四种类型
            if (rrs.getCanRunInReadDB() != null) {
                node.setCanRunInReadDB(rrs.getCanRunInReadDB());
            }
            if (rrs.getRunOnSlave() != null) {
                nodes[0].setCanRunSlave(rrs.getRunOnSlave());
            }
            nodes[i++] = node;
        }
        rrs.setNodes(nodes);
        return rrs;
    }


    // desc 的处理, 随机选择一个节点进行处理
    // show tables [from *] 的相关的处理
    // single node 的支持
    public static void routeForTableMeta(RouteResultset rrs,
                                         SchemaConfig schema, String tableName, String sql) {
        String dataNode = "";
//        String dataNode = null;
//        if (isNoSharding(schema, tableName)) {
//            //不分库的直接从schema中获取dataNode
//            dataNode = schema.getDataNode();
//        } else {
//            // ?
//            dataNode = getMetaReadDataNode(schema, tableName);
//        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        // readdb slavedb => 一般来说，读写分离 注定了 read db 是slave db
        // TODO: ensure the database name
        nodes[0] = new RouteResultsetNode(dataNode, "", sql);
//        if (rrs.getCanRunInReadDB() != null) {
//            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
//        }
//        if (rrs.getRunOnSlave() != null) {
//            nodes[0].setRunOnSlave(rrs.getRunOnSlave());
//        }
        rrs.setNodes(nodes);
    }

    /**
     * 根据标名随机获取一个节点 => 获取数据库的表的相关数据
     *
     * @param schema 数据库名
     * @param table  表名
     * @return 数据节点
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
//            dataNode = tc.getRandomDataNode();
            dataNode = tc.getDatasource().get(0).getDatanode();
        }
        return dataNode;
    }


    /**
     * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
     */
//    public static Set<String> ruleCalculate(TableConfig tc,
//                                            Set<ColumnRoutePair> colRoutePairSet) {
//        Set<String> routeNodeSet = new LinkedHashSet<String>();
//        String col = tc.getRule().getColumn();
//        PartitionConfig rule = tc.getRule();
//        AbstractPartition algorithm = rule.getPartition();
//        for (ColumnRoutePair colPair : colRoutePairSet) {
//            if (colPair.colValue != null) {
//                int nodeIndx = algorithm.caculate(colPair.colValue);
//                if (nodeIndx == null) {
//                    throw new IllegalArgumentException(
//                            "can't find datanode for sharding column:" + col
//                                    + " val:" + colPair.colValue);
//                } else {
//                    String dataNode = tc.getDataNodes().get(nodeIndx);
//                    routeNodeSet.add(dataNode);
//                    colPair.setNodeId(nodeIndx);
//                }
//            } else if (colPair.rangeValue != null) {
//                Integer[] nodeRange = algorithm.calculateRange(
//                        String.valueOf(colPair.rangeValue.beginValue),
//                        String.valueOf(colPair.rangeValue.endValue));
//                if (nodeRange != null) {
//                    /**
//                     * 不能确认 colPair的 nodeid是否会有其它影响
//                     */
//                    if (nodeRange.length == 0) {
//                        routeNodeSet.addAll(tc.getDataNodes());
//                    } else {
//                        ArrayList<String> dataNodes = tc.getDataNodes();
//                        String dataNode = null;
//                        for (Integer nodeId : nodeRange) {
//                            dataNode = dataNodes.get(nodeId);
//                            routeNodeSet.add(dataNode);
//                        }
//                    }
//                }
//            }
//
//        }
//        return routeNodeSet;
//    }

    /**
     * 应该不用去支持多表路由的
     * 但表的已经很复杂了
     * 多表路由
     * 根据计算单元, 获取所有的table!
     */
    public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx,
                                                   RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect)
            throws SQLNonTransientException {

        List<String> tables = ctx.getTables();

//        暂时不管里 nosharding 的库和表
//        if (schema.isNoSharding() || (tables.size() >= 1 && isNoSharding(schema, tables.get(0)))) {
//            return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
//        }

        //只有一个表的
        // 测试一下 多表的操作， 不过一般情况性爱，确实都是一张表的操作
        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schema, ctx, routeUnit, tables.get(0), rrs, isSelect);
        }

        Set<TableConfig.NodeConfig> retNodesSet = new HashSet<>();
        // 每个表对应的路由映射
        // 可能存在分片导致的路由
        Map<String, Set<TableConfig.NodeConfig>> tablesRouteMap = new HashMap<>();

        // 分库解析信息不为空
        Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();

//        tables where 类型的语句
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //为分库表找路由
            RouterUtil.findRouteWithcConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, ctx.getSql(), isSelect);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        // 遍历表
        for (String tableName : tables) {
            TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
            // 这种算是严重异常!
            if (tableConfig == null) {
                String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
                logger.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            if (tablesRouteMap.get(tableName) == null) {
                // 这个是什么情况
                // 余下的表都是单库表
                tablesRouteMap.put(tableName, new HashSet<TableConfig.NodeConfig>());
                tablesRouteMap.get(tableName).addAll(tableConfig.getDatasource());
            }
        }

        // 确保多表的交集
        boolean isFirstAdd = true;
        for (Map.Entry<String, Set<TableConfig.NodeConfig>> entry : tablesRouteMap.entrySet()) {
            if (entry.getValue() == null || entry.getValue().size() == 0) {
                throw new SQLNonTransientException("parent key can't find any valid datanode ");
            } else {
                // 区分了 第一次
                if (isFirstAdd) {
                    retNodesSet.addAll(entry.getValue());
                    isFirstAdd = false;
                } else {
                    // 保留交集
                    retNodesSet.retainAll(entry.getValue());
                    // 这个操作了复杂的多表操作
                    if (retNodesSet.size() == 0) {
                        //两个表的路由无交集
                        String errMsg = "invalid route in sql, multi tables found but datanode has no intersection "
                                + " sql:" + ctx.getSql();
                        logger.warn(errMsg);
                        throw new SQLNonTransientException(errMsg);
                    }
                }
            }
        }

        // 最终的检查
        if (retNodesSet != null && retNodesSet.size() > 0) {
            String tableName = tables.get(0);
            // 不止一列相交
            if (retNodesSet.size() > 1) {
                // mulit routes ,not cache route result
                if (isSelect) {
                    routeToSingleNode(rrs, retNodesSet.iterator().next().getDatanode(), ctx.getSql());
                } else {//delete 删除全局表的记录
//                    routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql(), true);
                    routeToMultiNode(rrs, retNodesSet, ctx.getSql());
                }

            } else {
//                routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql());
                routeToMultiNode(rrs, retNodesSet, ctx.getSql());
            }

        }
        return rrs;

    }


    /**
     * 单表路由
     */
    public static RouteResultset tryRouteForOneTable(SchemaConfig schema, DruidShardingParseInfo ctx,
                                                     RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs, boolean isSelect) throws SQLNonTransientException {

//        if (isNoSharding(schema, tableName)) {
//            return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
//        }

        TableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }


        if (!checkRuleRequired(schema, ctx, routeUnit, tc)) {
            throw new IllegalArgumentException("route rule for table "
                    + tc.getName() + " is required: " + ctx.getSql());

        }
        // 路由到所有表,
        if (tc.getPartitionColumn() == null) {
//            不应该支持 没有 partitionColumn 的情况
            //单表且不是childTable
//            tc.getDatasource().get(0).getDatanode();
//            return routeToMultiNode(rrs, tc.getDataNodes(), ctx.getSql());

//            tc.getDatasource().forEach(item ->
//            );
//            需要去改写代码
            return routeToMultiNode(rrs, tc.getDatasource(), ctx.getSql());
        } else {
            //每个表对应的路由映射
            Map<String, Set<TableConfig.NodeConfig>> tablesRouteMap = new HashMap<>();
            if (routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
                RouterUtil.findRouteWithcConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, ctx.getSql(), isSelect);
                if (rrs.isFinishedRoute()) {
                    return rrs;
                }
            }

            if (tablesRouteMap.get(tableName) == null) {
                return routeToMultiNode(rrs, tc.getDatasource() , ctx.getSql());
            } else {
                return routeToMultiNode(rrs, tablesRouteMap.get(tableName), ctx.getSql());
            }
        }
    }


    /**
     * 处理分库表路由
     */
    public static void findRouteWithcConditionsForTables(SchemaConfig schema, RouteResultset rrs,
                                                         Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
                                                         Map<String, Set<TableConfig.NodeConfig>> tablesRouteMap, String sql, boolean isSelect)
            throws SQLNonTransientException {

        //为分库表找路由
        for (Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            String tableName = entry.getKey().toUpperCase();
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                String msg = "can't find table define in schema "
                        + tableName + " schema:" + schema.getName();
                logger.warn(msg);
                throw new SQLNonTransientException(msg);
            }

            // 全局表或者不分库的表略过（全局表后面再计算）
            // 全局表不用管，暂时不支持, 这里处理单个表, 暂时也不用管
            // table的node 只有一个
            if (schema.getTables().get(tableName).getDatasource().size() == 1) {
                continue;
            } else {
                //非全局表：分库表、childTable、其他
                Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
                String partionCol = tableConfig.getPartitionColumn();
                String primaryKey = tableConfig.getPrimaryKey();

                boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;

                // 有主键，并且就一个
                // 这个操作了什么 ?
                // 貌似可以进行删除
                if (entry.getValue().get(primaryKey) != null && entry.getValue().size() == 1) {
                    //主键查找
                    // try by primary key if found in cache
                    Set<ColumnRoutePair> primaryKeyPairs = entry.getValue().get(primaryKey);
                    if (primaryKeyPairs != null) {
                        String tableKey = schema.getName() + '_' + tableName;
                        boolean allFound = false;

                        // need cache primary key -> datanode relation
                        if (isSelect && tableConfig.getPrimaryKey() != null) {
                            rrs.setPrimaryKey(tableKey + '.' + tableConfig.getPrimaryKey());
                        }
                    }
                }


                // select 语句中包含 partitionColumn,
                // 计算partition,
                if (isFoundPartitionValue) {
                    // 获取 partitionColumn
                    Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
                    // 一个col, 有这么多 pair!
                    for (ColumnRoutePair pair : partitionValue) {

                        // 两种情况 : columnValue RangeValue
                        if (pair.colValue != null) {
                            Integer nodeIndex = tableConfig.getRule().getPartition().caculate(pair.colValue);
                            if (nodeIndex == null) {
                                String msg = "can't find any valid datanode :" + tableConfig.getName()
                                        + " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                                logger.warn(msg);
                                throw new SQLNonTransientException(msg);
                            }

                            List<TableConfig.NodeConfig> dataNodes = tableConfig.getDatasource();
                            TableConfig.NodeConfig node;

                            if (nodeIndex >= 0 && nodeIndex < dataNodes.size()) {
                                node = dataNodes.get(nodeIndex);
                            } else {
                                node = null;
                                String msg = "Can't find a valid data node for specified node index :"
                                        + tableConfig.getName() + " -> " + tableConfig.getPartitionColumn()
                                        + " -> " + pair.colValue + " -> " + "Index : " + nodeIndex;
                                logger.warn(msg);
                                throw new SQLNonTransientException(msg);
                            }

                            // 这个有点奇怪
                            if (node != null) {
                                if (tablesRouteMap.get(tableName) == null) {
                                    tablesRouteMap.put(tableName, new HashSet<TableConfig.NodeConfig>());
                                }
                                tablesRouteMap.get(tableName).add(node);
                            }
                        }

                        // range类型
                        if (pair.rangeValue != null) {
                            int[] nodeIndexs = tableConfig.getRule().getPartition()
                                    .calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
                            List<TableConfig.NodeConfig> dataNodes = tableConfig.getDatasource();
                            TableConfig.NodeConfig node;
                            for (Integer idx : nodeIndexs) {
                                if (idx >= 0 && idx < dataNodes.size()) {
                                    node = dataNodes.get(idx);
                                } else {
                                    String msg = "Can't find valid data node(s) for some of specified node indexes :"
                                            + tableConfig.getName() + " -> " + tableConfig.getPartitionColumn();
                                    logger.warn(msg);
                                    throw new SQLNonTransientException(msg);
                                }
                                if (node != null) {
                                    if (tablesRouteMap.get(tableName) == null) {
                                        tablesRouteMap.put(tableName, new HashSet<TableConfig.NodeConfig>());
                                    }
                                    tablesRouteMap.get(tableName).add(node);

                                }
                            }
                        }
                    }
                } else {
                    //没找到拆分字段，该表的所有节点都路由
                    if (tablesRouteMap.get(tableName) == null) {
                        tablesRouteMap.put(tableName, new HashSet<TableConfig.NodeConfig>());
                    }
                    List<String> items = new ArrayList<>();
                    tableConfig.getDatasource().forEach(item ->
                    {
                        items.add(item.getDatanode());
                    });
                    tablesRouteMap.get(tableName).addAll(tableConfig.getDatasource());
                }
            }
        }
    }

    //
//    /**
//     * @param schema
//     * @param ctx
//     * @param tc
//     * @return true表示校验通过，false表示检验不通过
//     * <p>
//     * 后期支持 rule 不需要校验
//     */
    public static boolean checkRuleRequired(SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, TableConfig tc) {
//        if (!tc.isRuleRequired()) {
//            return true;
//        }
        boolean hasRequiredValue = false;
        String tableName = tc.getName();
        if (routeUnit.getTablesAndConditions().get(tableName) == null || routeUnit.getTablesAndConditions().get(tableName).size() == 0) {
            hasRequiredValue = false;
        } else {
            for (Map.Entry<String, Set<ColumnRoutePair>> condition : routeUnit.getTablesAndConditions().get(tableName).entrySet()) {

                String colName = condition.getKey();
                //条件字段是拆分字段
                if (colName.equals(tc.getPartitionColumn())) {
                    hasRequiredValue = true;
                    break;
                }
            }
        }
        return hasRequiredValue;
    }

    /**
     * 增加判断支持未配置分片的表走默认的dataNode
     * 后期添加 no-sharding 的支持
     *
     * @param schemaConfig
     * @param tableName
     * @return
     */
    public static boolean isNoSharding(SchemaConfig schemaConfig, String tableName) {
        // Table名字被转化为大写的，存储在schema

//        tableName = tableName.toUpperCase();
//        if (schemaConfig.isNoSharding()) {
//            return true;
//        }
//
//        if (schemaConfig.getDataNode() != null && !schemaConfig.getTables().containsKey(tableName)) {
//            return true;
//        }

        return false;
    }

    /**
     * 判断条件是否永真
     *
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysTrue(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        if (Boolean.TRUE.equals(o)) {
            return true;
        }
        return false;
    }

    /**
     * 判断条件是否永假的
     *
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysFalse(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        if (Boolean.FALSE.equals(o)) {
            return true;
        }
        return false;
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
