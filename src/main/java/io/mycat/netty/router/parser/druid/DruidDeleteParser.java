package io.mycat.netty.router.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.conf.TableConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Objects;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidDeleteParser extends DefaultDruidParser {
    private static final Logger logger = LoggerFactory.getLogger(DruidDeleteParser.class);

    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        MySqlDeleteStatement delete = (MySqlDeleteStatement) stmt;
        String tableName = StringUtil.removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
//        visitor can fidn table name
//        ctx.addTable(tableName);


        TableConfig tc = schema.getTables().get(tableName);
        // TODO: 提出相似的代码
        if (Objects.isNull(tc)) {
            String msg = "can't find table : " + tableName + " define in schema : " + schema.getName();
            logger.warn(msg);
            throw new IllegalArgumentException(msg);
        }
    }
}
