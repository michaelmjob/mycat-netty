package io.mycat.netty.router.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.router.RouteResultset;
import io.mycat.netty.router.parser.util.DruidShardingParseInfo;
import io.mycat.netty.router.parser.util.MycatSchemaStatVisitor;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/8/27.
 */
public interface DruidParser {
    /**
     * 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
     *
     * @param schema
     * @param stmt
     */
    void parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, MycatSchemaStatVisitor schemaStatVisitor) throws SQLNonTransientException;

    /**
     * statement方式解析
     * 子类可覆盖（如果visitorParse解析得不到表名、字段等信息的，就通过覆盖该方法来解析）
     * 子类覆盖该方法一般是将SQLStatement转型后再解析（如转型为MySqlInsertStatement）
     */
     void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;

    /**
     * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementPparse去解析）
     * 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
     *
     * @param stmt
     */
     void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) throws SQLNonTransientException;

    /**
     * 改写sql：加limit，加group by、加order by如有些没有加limit的可以通过该方法增加
     *
     * @param schema
     * @param rrs
     * @param stmt
     * @throws SQLNonTransientException
     */
     void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;

    /**
     * 获取解析到的信息
     *
     * @return
     */
     DruidShardingParseInfo getCtx();
}
