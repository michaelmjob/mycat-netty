package io.mycat.netty.router;

import io.mycat.netty.mysql.MysqlSessionContext;

import java.sql.SQLNonTransientException;

/**
 * Created by snow_young on 16/8/22.
 */
public interface RouteStrategy {

    public RouteResultset route(int sqlType, String origSQL, MysqlSessionContext mysqlSessionContext)
            throws SQLNonTransientException;


}
