package io.mycat.netty.router.parser.druid.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.druid.StringUtil;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidDeleteParser extends DefaultDruidParser {
    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        MySqlDeleteStatement delete = (MySqlDeleteStatement)stmt;
        String tableName = StringUtil.removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
        ctx.addTable(tableName);
    }
}
