package io.mycat.netty.router;

import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.datasource.Host;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by snow_young on 16/8/12.
 */
public class RouteResultsetNode implements Serializable, Comparator<RouteResultsetNode>{
    private static final Logger logger = LoggerFactory.getLogger(RouteResultsetNode.class);

    private static final long seriaVersionUID = 1;
    @Getter
    private String dataNodeName;
    @Getter
    private String statement;
    @Getter
    private String database;

    @Getter
    private String sql;
    private int sqlType;

    private int limitStart;
    private int limitSize;

    // 是否强制走master
    @Getter
    @Setter
    private Boolean canRunSlave = true;

    @Getter
    @Setter
    private boolean canRunInReadDB = false;
    // 添加负载均衡标志
    // boolean hasBalanceFlag = ??

    @Getter
    @Setter
    private Host host;



    public RouteResultsetNode(String dataNodeName, String databaseName, String sql){
        this.dataNodeName = dataNodeName;
        this.database = databaseName;
        this.sql = sql;
    }

    @Override
    public int compare(RouteResultsetNode o1, RouteResultsetNode o2) {
        return 1;

    }
}
