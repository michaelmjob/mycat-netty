package io.mycat.netty.router;

import io.mycat.netty.Session;
import io.mycat.netty.mysql.MysqlSessionContext;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/8/12.
 */
@NoArgsConstructor
public class RouteService {
    private static final Logger logger = LoggerFactory.getLogger(RouteService.class);

    public RouteResultset route(int sqlType, String stmt, MysqlSessionContext mysqlSessionContext){



        return null;
    }

}
