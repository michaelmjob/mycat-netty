package io.mycat.netty.router.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.util.MycatSchemaStatVisitor;
import org.junit.Test;

/**
 * Created by snow_young on 16/8/28.
 */
public class DruidSelectParserTest {

    @Test
    public void testSelect(){

        String stmt = "select order_date from order where order_id = 2015-06-06";
        SQLStatementParser parser = new MySqlStatementParser(stmt);

        SQLStatement statement = parser.parseStatement();
        MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();


        // druidParser.parser(schema, rrs, statement, stmt, visitor);
        
    }
}
