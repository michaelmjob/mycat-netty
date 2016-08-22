package io.mycat.netty.router.interceptor;

/**
 * Created by snow_young on 16/8/22.
 */
public interface SQLInterceptor {

    /**
     * return new sql to handler,ca't modify sql's type
     * @param sql
     * @param sqlType
     * @return new sql
     */
    String interceptSQL(String sql ,int sqlType);
}
