package io.mycat.netty.router.parser.util;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by snow_young on 16/8/27.
 */
public class DruidParserFactory {
    private static final Logger logger = LoggerFactory.getLogger(DruidParserFactory.class);

    public static DruidParser create(SchemaConfig schema, SQLStatement statement, SchemaStatVisitor visitor) {
        DruidParser parser = null;

        if (statement instanceof SQLSelectStatement) {
            parser = new DruidSelectParser();
        } else if (statement instanceof MySqlInsertStatement) {
            parser = new DruidInsertParser();
        } else if (statement instanceof MySqlDeleteStatement) {
            parser = new DruidDeleteParser();
        } else if (statement instanceof MySqlCreateTableStatement) {
            parser = new DruidCreateTableParser();
        } else if (statement instanceof MySqlUpdateStatement) {
            parser = new DruidUpdateParser();
        } else if (statement instanceof SQLAlterTableStatement) {
            parser = new DruidAlterTableParser();
        } else {
            parser = new DefaultDruidParser();
        }

        return parser;
    }

    private static List<String> parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor) {
        List<String> tables = new ArrayList<>();
        // 使用 visitor 遍历 SQLStatement !!!
        stmt.accept(schemaStatVisitor);

        if (schemaStatVisitor.getAliasMap() != null) {
            for (Map.Entry<String, String> entry : schemaStatVisitor.getAliasMap().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (value != null && value.indexOf("`") >= 0) {
                    value = value.replaceAll("`", "");
                }
                //表名前面带database的，去掉
                if (key != null) {
                    int pos = key.indexOf("`");
                    if (pos > 0) {
                        key = key.replaceAll("`", "");
                    }
                    pos = key.indexOf(".");
                    if (pos > 0) {
                        key = key.substring(pos + 1);
                    }

                    if (key.equals(value)) {
                        tables.add(key.toUpperCase());
                    }
                }
            }

        }
        return tables;
    }
}
