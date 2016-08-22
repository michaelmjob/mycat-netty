package io.mycat.netty.router;

import io.mycat.netty.mysql.MysqlSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/8/22.
 */
public class AbstractRouteStrategy implements RouteStrategy{
    private static final Logger logger = LoggerFactory.getLogger(AbstractRouteStrategy.class);


    @Override
    public RouteResultset route(int sqlType, String origSQL, MysqlSessionContext mysqlSessionContext) throws SQLNonTransientException {

        // beforeRouteProcess, 全局表的逻辑

        // sql inteceptor 逻辑

        // ddl语句的支持！

        // 分片的支持

//        RouteResultset returnedSet = routeSystemInfo(schema, sqlType, stmt, rrs);
//        if (returnedSet == null) {
//            rrs = routeNormalSqlWithAST(schema, stmt, rrs, charset, cachePool);
//        }
        

        return null;
    }
}
