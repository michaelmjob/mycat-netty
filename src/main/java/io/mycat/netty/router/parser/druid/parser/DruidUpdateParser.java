package io.mycat.netty.router.parser.druid.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.conf.TableConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.druid.RouterUtil;
import io.mycat.netty.router.parser.druid.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.List;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidUpdateParser extends DefaultDruidParser {
    private static final Logger logger = LoggerFactory.getLogger(DruidUpdateParser.class);

    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        //这里限制了update分片表的个数只能有一个
//        if (ctx.getTables() != null && ctx.getTables().size() > 1 && !schema.isNoSharding()) {
        if (ctx.getTables() != null && ctx.getTables().size() > 1) {
            String msg = "multi table related update not supported,tables:" + ctx.getTables();
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        MySqlUpdateStatement update = (MySqlUpdateStatement) stmt;
        String tableName = StringUtil.removeBackquote(update.getTableName().getSimpleName().toUpperCase());

        List<SQLUpdateSetItem> updateSetItem = update.getItems();
        TableConfig tc = schema.getTables().get(tableName);

        if (RouterUtil.isNoSharding(schema, tableName)) {//整个schema都不分库或者该表不拆分
            RouterUtil.routeForTableMeta(rrs, schema, tableName, rrs.getStatement());
            rrs.setFinishedRoute(true);
            return;
        }

        String partitionColumn = tc.getPartitionColumn();


        confirmShardColumnNotUpdated(updateSetItem, schema, tableName, partitionColumn, rrs);

//		if(ctx.getTablesAndConditions().size() > 0) {
//			Map<String, Set<ColumnRoutePair>> map = ctx.getTablesAndConditions().get(tableName);
//			if(map != null) {
//				for(Map.Entry<String, Set<ColumnRoutePair>> entry : map.entrySet()) {
//					String column = entry.getKey();
//					Set<ColumnRoutePair> value = entry.getValue();
//					if(column.toUpperCase().equals(anObject))
//				}
//			}
//			
//		}
//		System.out.println();

//        暂时不支持 global table
//        if (schema.getTables().get(tableName).isGlobalTable() && ctx.getRouteCalculateUnit().getTablesAndConditions().size() > 1) {
//            throw new SQLNonTransientException("global table not supported multi table related update " + tableName);
//        }
    }

    private void confirmShardColumnNotUpdated(List<SQLUpdateSetItem> updateSetItem, SchemaConfig schema, String tableName, String partitionColumn, RouteResultset rrs) throws SQLNonTransientException {
        if (updateSetItem != null && updateSetItem.size() > 0) {
            // 父表是什么
//            boolean hasParent = (schema.getTables().get(tableName).getParentTC() != null);
            for (SQLUpdateSetItem item : updateSetItem) {
                String column = StringUtil.removeBackquote(item.getColumn().toString().toUpperCase());
                //考虑别名，前面已经限制了update分片表的个数只能有一个，所以这里别名只能是分片表的
                if (column.contains(StringUtil.TABLE_COLUMN_SEPARATOR)) {
                    column = column.substring(column.indexOf(".") + 1).trim().toUpperCase();
                }
                if (partitionColumn != null && partitionColumn.equals(column)) {
                    String msg = "partion key can't be updated " + tableName + "->" + partitionColumn;
                    logger.warn(msg);
                    throw new SQLNonTransientException(msg);
                }
            }
        }
    }
}
