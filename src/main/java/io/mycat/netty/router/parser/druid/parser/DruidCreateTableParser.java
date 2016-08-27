package io.mycat.netty.router.parser.druid.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.druid.MycatSchemaStatVisitor;
import io.mycat.netty.router.parser.druid.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidCreateTableParser extends DefaultDruidParser {
    private static final Logger logger = LoggerFactory.getLogger(DruidCreateTableParser.class);

    @Override
    public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) {
    }

    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement)stmt;
        if(createStmt.getQuery() != null) {
            String msg = "create table from other table not supported :" + stmt;
            logger.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        String tableName = StringUtil.removeBackquote(createStmt.getTableSource().toString().toUpperCase());
        ctx.addTable(tableName);

    }
}
