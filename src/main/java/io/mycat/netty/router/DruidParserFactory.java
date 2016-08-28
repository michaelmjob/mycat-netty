package io.mycat.netty.router;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.router.parser.druid.*;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidParserFactory {

    public static DruidParser create(SchemaConfig schema, SQLStatement statement, SchemaStatVisitor visitor)
    {
        DruidParser parser = null;
        if (statement instanceof SQLSelectStatement)
        {
//            if(schema.isNeedSupportMultiDBType())
//            {
//                parser = getDruidParserForMultiDB(schema, statement, visitor);
//
//            }

            if (parser == null)
            {
                parser = new DruidSelectParser();
            }
        } else if (statement instanceof MySqlInsertStatement)
        {
            parser = new DruidInsertParser();
        } else if (statement instanceof MySqlDeleteStatement)
        {
            parser = new DruidDeleteParser();
        } else if (statement instanceof MySqlCreateTableStatement)
        {
            parser = new DruidCreateTableParser();
        } else if (statement instanceof MySqlUpdateStatement)
        {
            parser = new DruidUpdateParser();
        } else if (statement instanceof SQLAlterTableStatement)
        {
            parser = new DruidAlterTableParser();
        } else
        {
            parser = new DefaultDruidParser();
        }

        return parser;
    }
}
