package io.mycat.netty.router;

import io.mycat.netty.conf.Configuration;
import io.mycat.netty.conf.SchemaConfig;
import io.mycat.netty.mysql.MysqlSessionContext;
import io.mycat.netty.mysql.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

/**
 * Created by snow_young on 16/8/22.
 */
public abstract class AbstractRouteStrategy implements RouteStrategy{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRouteStrategy.class);


    @Override
    public RouteResultset route(int sqlType, String origSQL, MysqlSessionContext mysqlSessionContext) throws SQLNonTransientException {

        // beforeRouteProcess, 全局表的逻辑

        // sql inteceptor 逻辑

        // ddl语句的支持！

        // 分片的支持

        //对应schema标签checkSQLschema属性，把表示schema的字符去掉
//        if (schema.isCheckSQLSchema()) {
//            stmt = RouterUtil.removeSchema(stmt, schema.getName());
//        }

        String stmt = origSQL;
        RouteResultset rrs = new RouteResultset(stmt, sqlType);

        if (mysqlSessionContext != null ) {
            rrs.setAutocommit(mysqlSessionContext.getFrontSession().isAutocommit());
        }

        /**
         * DDL 语句的路由
         */
//        if (ServerParse.DDL == sqlType) {
//            return RouterUtil.routeToDDLNode(rrs, sqlType, stmt, schema);
//        }


        String schema = mysqlSessionContext.getFrontSession().getSchema().toUpperCase();
        SchemaConfig schemaConfig = Configuration.getSchemaCofnigs().get(schema);
        String charset = mysqlSessionContext.getFrontSession().getCharset();

        // two steps.
        // system info is not necessary
        RouteResultset returnedSet = routeSystemInfo(schemaConfig, sqlType, stmt, rrs);
        if (returnedSet == null) {
            return routeNormalSqlWithAST(schemaConfig, stmt, rrs, charset);
        }

        return rrs;
    }



    /**
     * 路由之前必要的处理
     * 主要是全局序列号插入，还有子表插入
     */
//    private boolean beforeRouteProcess(SchemaConfig schema, int sqlType, String origSQL, ServerConnection sc)
//            throws SQLNonTransientException {
//    }

    /**
     * 通过解析AST语法树类来寻找路由
     */
    public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs,
                                                         String charset) throws SQLNonTransientException;

    /**
     * 路由信息指令, 如 SHOW、SELECT@@、DESCRIBE
     */
    public abstract RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType, String stmt, RouteResultset rrs)
            throws SQLSyntaxErrorException;

    /**
     * 解析 Show 之类的语句
     */
    public abstract RouteResultset analyseShowSQL(SchemaConfig schema, RouteResultset rrs, String stmt)
            throws SQLNonTransientException;

}
