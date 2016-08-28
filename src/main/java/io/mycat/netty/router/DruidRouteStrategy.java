package io.mycat.netty.router;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.parser.ServerParse;
import io.mycat.netty.router.parser.druid.DruidShardingParseInfo;
import io.mycat.netty.router.parser.druid.MycatSchemaStatVisitor;
import io.mycat.netty.router.parser.druid.RouteCalculateUnit;
import io.mycat.netty.router.parser.druid.RouterUtil;
import io.mycat.netty.router.parser.druid.parser.DruidParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by snow_young on 16/8/22.
 */
public class DruidRouteStrategy extends AbstractRouteStrategy {
    private static Logger logger = LoggerFactory.getLogger(DruidRouteStrategy.class);


    @Override
    public RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs, String charset) throws SQLNonTransientException {

        return null;
//        SQLStatementParser parser = new MySqlStatementParser(stmt);
//
//        SQLStatement statement = null;
//
//        statement = parser.parseStatement();
//        MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();
//
//
//        DruidParser druidParser = DruidParserFactory.create(schema, statement, visitor);
//        druidParser.parser(schema, rrs, statement, stmt, visitor);
//
//        /**
//         * DruidParser 解析过程中已完成了路由的直接返回
//         * 什么时候会没有完成呢 ?
//         * 暂时用不到
//         */
//        if (rrs.isFinishedRoute()) {
//            return rrs;
//        }
//
//        /**
//         * 没有from的select语句或其他 ?? 是什么意思，就是没有具体指定一个 数据库 的操作？
//         * 怎么先添加一个测试
//         */
//        DruidShardingParseInfo ctx = druidParser.getCtx();
////        这应该算是一个异常!!!
////        if ((ctx.getTables() == null || ctx.getTables().size() == 0) && (ctx.getTableAliasMap() == null || ctx.getTableAliasMap().isEmpty())) {
////            //
////            return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), druidParser.getCtx().getSql());
////        }
//
//        // 空的路由计算 就放一个空的
//        // 算不算一个异常
//        if (druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
//            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
//            druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
//        }
//
//        // 获取所有的路由计算单元,
//        // 计算路由， 放入nodeset
//        // 一个 RouteCalculateUnit 对应一个 RouteResultset
//        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<RouteResultsetNode>();
//        for (RouteCalculateUnit unit : druidParser.getCtx().getRouteCalculateUnits()) {
//            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement));
//            if (rrsTmp != null) {
//                for (RouteResultsetNode node : rrsTmp.getNodes()) {
//                    nodeSet.add(node);
//                }
//            }
//        }
//
//        // 转换格式 set -> array
//        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
//        int i = 0;
//        for (Iterator<RouteResultsetNode> iterator = nodeSet.iterator(); iterator.hasNext(); ) {
//            nodes[i] = iterator.next();
//            i++;
//        }
//        rrs.setNodes(nodes);
//
//        return rrs;
    }

    private boolean isSelect(SQLStatement statement) {
        if (statement instanceof SQLSelectStatement) {
            return true;
        }
        return false;
    }

    @Override
    public RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType, String stmt, RouteResultset rrs) throws SQLSyntaxErrorException {
        switch (sqlType) {
            case ServerParse.SHOW:// if origSQL is like show tables
                return analyseShowSQL(schema, rrs, stmt);
            case ServerParse.SELECT://if origSQL is like select @@
                if (stmt.contains("@@")) {
                    return analyseDoubleAtSgin(schema, rrs, stmt);
                }
                break;
            case ServerParse.DESCRIBE:// if origSQL is meta SQL, such as describe table
                int ind = stmt.indexOf(' ');
                stmt = stmt.trim();
                return analyseDescrSQL(schema, rrs, stmt, ind + 1);
        }
        return null;
    }

    @Override
    public RouteResultset analyseShowSQL(SchemaConfig schema, RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
        return null;
    }

    /**
     * 对Desc语句进行分析 返回数据路由集合， 从 datanode中 随机选择一个节点进行处理
     * *
     *
     * @param schema 数据库名
     * @param rrs    数据路由集合
     * @param stmt   执行语句
     * @param ind    第一个' '的位置
     * @return RouteResultset        (数据路由集合)
     * @author mycat
     */
    private static RouteResultset analyseDescrSQL(SchemaConfig schema,
                                                  RouteResultset rrs, String stmt, int ind) {

//        return null;
        final String MATCHED_FEATURE = "DESCRIBE ";
        final String MATCHED2_FEATURE = "DESC ";
        int pos = 0;
        while (pos < stmt.length()) {
            char ch = stmt.charAt(pos);
            // 忽略处理注释 /* */ BEN
            if (ch == '/' && pos + 4 < stmt.length() && stmt.charAt(pos + 1) == '*') {
                if (stmt.substring(pos + 2).indexOf("*/") != -1) {
                    pos += stmt.substring(pos + 2).indexOf("*/") + 4;
                    continue;
                } else {
                    // 不应该发生这类情况。
                    throw new IllegalArgumentException("sql 注释 语法错误");
                }
            } else if (ch == 'D' || ch == 'd') {
                // 匹配 [describe ]
                if (pos + MATCHED_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED_FEATURE) != -1)) {
                    pos = pos + MATCHED_FEATURE.length();
                    break;
                } else if (pos + MATCHED2_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED2_FEATURE) != -1)) {
                    pos = pos + MATCHED2_FEATURE.length();
                    break;
                } else {
                    pos++;
                }
            }
        }

        // 重置ind坐标。BEN GONG
        ind = pos;
        int[] repPos = {ind, 0};
        String tableName = RouterUtil.getTableName(stmt, repPos);

        stmt = stmt.substring(0, ind) + tableName + stmt.substring(repPos[1]);
        RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
        return rrs;
    }

    /**
     * 根据执行语句判断数据路由
     *
     * @param schema 数据库名
     * @param rrs    数据路由集合
     * @param stmt   执行sql
     * @return RouteResultset        数据路由集合
     * @throws SQLSyntaxErrorException
     * @author mycat
     */
    private RouteResultset analyseDoubleAtSgin(SchemaConfig schema,
                                               RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
//        String upStmt = stmt.toUpperCase();
//        int atSginInd = upStmt.indexOf(" @@");
//        if (atSginInd > 0) {
//            return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
//        }
        return null;
//        return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
    }
}
