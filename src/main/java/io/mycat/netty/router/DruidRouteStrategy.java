package io.mycat.netty.router;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.mysql.parser.ServerParse;
import io.mycat.netty.router.parser.druid.DruidParser;

import io.mycat.netty.router.parser.util.DruidShardingParseInfo;
import io.mycat.netty.router.parser.util.MycatSchemaStatVisitor;
import io.mycat.netty.router.parser.util.RouteCalculateUnit;
import io.mycat.netty.router.parser.util.RouterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by snow_young on 16/8/22.
 *
 * 这个不可测试
 */
public class DruidRouteStrategy extends AbstractRouteStrategy {
    private static Logger logger = LoggerFactory.getLogger(DruidRouteStrategy.class);


    @Override
    public RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs, String charset) throws SQLNonTransientException {

        SQLStatementParser parser = new MySqlStatementParser(stmt);

        SQLStatement statement = parser.parseStatement();
        MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();

        /**
         * 检验unsupported statement
         */
//        checkUnSupportedStatement(statement);

        // 就是这里需要进行测试的点
        DruidParser druidParser = DruidParserFactory.create(schema, statement, visitor);
        druidParser.parser(schema, rrs, statement, stmt, visitor);

        /**
         * DruidParser 解析过程中已完成了路由的直接返回
         * ???
         */
//        if (rrs.isFinishedRoute()) {
//            return rrs;
//        }
//        rrs = new RouteResultset();

        /**
         * 最简单的select语句
         */
//        DruidShardingParseInfo ctx = druidParser.getCtx();
//        最简单的sql语句, 暂时就不支持了, 后面添加
//        if ((ctx.getTables() == null || ctx.getTables().size() == 0) && (ctx.getTableAliasMap() == null || ctx.getTableAliasMap().isEmpty())) {
//            //
//            return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), druidParser.getCtx().getSql());
//        }

        // 空的路由计算 就放一个空的
        if (druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
        }

        // 获取所有的路由计算单元,
        // 计算路由, 放入nodeset
        // 一个 RouteCalculateUnit 对应一个 RouteResultset
        // 获取路由结果
        // 真的需要排序吗 ?
//        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
        for (RouteCalculateUnit unit : druidParser.getCtx().getRouteCalculateUnits()) {

            // 目前直接修改 rrss 实现，弊端，不能够去重
            RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement));
//            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement));
//            if (rrsTmp != null) {
//                for (RouteResultsetNode node : rrsTmp.getNodes()) {
//                    logger.info("node ele : {}", node);
//                    logger.info(node.getDatabase() + "; " + node.getDataNodeName() + "; " + node.getSql());
//                    nodeSet.add(node);
//                }
//            }
        }

////        rrs.clearNodes();
//        nodeSet.forEach(iter -> {
//            rrs.addNode(iter);
//        });

        return rrs;
    }

    private boolean isSelect(SQLStatement statement) {
        if (statement instanceof SQLSelectStatement) {
            return true;
        }
        return false;
    }

    // 暂时不测试这个功能
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
