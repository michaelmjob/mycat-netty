package io.mycat.netty.router.interceptor;

/**
 * Created by snow_young on 16/8/22.
 */
public class DefaultSqlInterceptor implements SQLInterceptor{

    // according to mycat implementation, this operation is extra.
    @Override
    public String interceptSQL(String sql, int sqlType) {
        return sql;
    }
}
