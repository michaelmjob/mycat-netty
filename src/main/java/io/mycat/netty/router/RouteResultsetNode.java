package io.mycat.netty.router;

import io.mycat.netty.mysql.backend.NettyBackendSession;
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

    private NettyBackendSession backendSession;


    public RouteResultsetNode(String dataNodeName, String statement){
        this.dataNodeName = dataNodeName;
        this.statement = statement;
        this.sqlType = sqlType;
    }

    //
    public void getBackendSession(){
        // get real node fro nodeName

    }

    @Override
    public int compare(RouteResultsetNode o1, RouteResultsetNode o2) {
        return 1;

    }
}
