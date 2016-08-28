package io.mycat.netty.router.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.util.MycatSchemaStatVisitor;
import io.mycat.netty.router.parser.util.StringUtil;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidAlterTableParser extends DefaultDruidParser {
    @Override
    public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) throws SQLNonTransientException {

    }

    @Override
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) stmt;
        String tableName = StringUtil.removeBackquote(alterTable.getTableSource().toString().toUpperCase());
        ctx.addTable(tableName);

    }

//    不应该是alter table 的处理吗 ?
//    public static void main(String[] args)
//    {
//        String s="SELECT Customer,SUM(OrderPrice) FROM Orders\n" +
//                "GROUP BY Customer";
//        SQLStatementParser parser = new MySqlStatementParser(s);
//        SQLStatement statement = parser.parseStatement();
//        System.out.println();
//    }
}
