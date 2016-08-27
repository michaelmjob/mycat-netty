package io.mycat.netty.router.parser.druid.parser;

import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.conf.TableConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.druid.MycatSchemaStatVisitor;
import io.mycat.netty.router.parser.druid.RouteCalculateUnit;
import io.mycat.netty.router.parser.druid.RouterUtil;
import io.mycat.netty.router.parser.druid.StringUtil;
import io.mycat.netty.router.partition.AbstractPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;


import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidInsertParser extends DefaultDruidParser {
    private static final Logger logger = LoggerFactory.getLogger(DruidInsertParser.class);

    @Override
    public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) throws SQLNonTransientException {

    }

    /**
     * 考虑因素：isChildTable、批量、是否分片
     * 目前只支持
     */
    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        MySqlInsertStatement insert = (MySqlInsertStatement) stmt;
        String tableName = StringUtil.removeBackquote(insert.getTableName().getSimpleName()).toUpperCase();

        ctx.addTable(tableName);

        TableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            String msg = "can't find table define in schema "
                    + tableName + " schema:" + schema.getName();
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        } else {

            // 后面添加两维度分表
            String partitionColumn = tc.getPartitionColumn();

            if (partitionColumn != null) {//分片表
                //拆分表必须给出column list,否则无法寻找分片字段的值
                if (insert.getColumns() == null || insert.getColumns().size() == 0) {
                    throw new SQLSyntaxErrorException("partition table, insert must provide ColumnList");
                }

                // 批量insert
                // 暂时不支持 多行插入
                parserSingleInsert(schema, rrs, partitionColumn, tableName, insert);
            }
        }
    }

    /**
     * 寻找joinKey的索引
     *
     * @param columns
     * @param joinKey
     * @return -1表示没找到，>=0表示找到了
     */
    private int getJoinKeyIndex(List<SQLExpr> columns, String joinKey) {
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
    private boolean isMultiInsert(MySqlInsertStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1) || insertStmt.getQuery() != null;
    }


    /**
     * 目前只支持这个操作
     * 单条insert（非批量）
     *
     * @param schema
     * @param rrs
     * @param partitionColumn
     * @param tableName
     * @param insertStmt
     * @throws SQLNonTransientException
     */
    private void parserSingleInsert(SchemaConfig schema, RouteResultset rrs, String partitionColumn,
                                    String tableName, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
        boolean isFound = false;
        // 将分片的键 作为 路由计算单元
        for (int i = 0; i < insertStmt.getColumns().size(); i++) {
            if (partitionColumn.equalsIgnoreCase(StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString()))) {
                //找到分片字段
                isFound = true;
                String column = StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString());

                String value = StringUtil.removeBackquote(insertStmt.getValues().getValues().get(i).toString());

                RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
                routeCalculateUnit.addShardingExpr(tableName, column, value);

                ctx.addRouteCalculateUnit(routeCalculateUnit);

                //mycat是单分片键，找到了就返回
                break;
            }
        }
        if (!isFound) {//分片表的
            String msg = "bad insert sql (sharding column:" + partitionColumn + " not provided," + insertStmt;
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        //  insert into .... on duplicateKey
        //  such as :   INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE b=VALUES(b);
        //              INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
        //              duplicate 处理！！
        if (insertStmt.getDuplicateKeyUpdate() != null) {
            List<SQLExpr> updateList = insertStmt.getDuplicateKeyUpdate();
            for (SQLExpr expr : updateList) {
                SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr) expr;
                String column = StringUtil.removeBackquote(opExpr.getLeft().toString().toUpperCase());
                if (column.equals(partitionColumn)) {
                    String msg = "partion key can't be updated: " + tableName + " -> " + partitionColumn;
                    logger.warn(msg);
                    throw new SQLNonTransientException(msg);
                }
            }
        }
    }

    /**
     * 寻找拆分字段在 columnList中的索引
     *
     * @param insertStmt
     * @param partitionColumn
     * @return
     */
    private int getShardingColIndex(MySqlInsertStatement insertStmt, String partitionColumn) {
        int shardingColIndex = -1;
        for (int i = 0; i < insertStmt.getColumns().size(); i++) {
            if (partitionColumn.equalsIgnoreCase(StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString()))) {//找到分片字段
                shardingColIndex = i;
                return shardingColIndex;
            }
        }
        return shardingColIndex;
    }
}