package io.mycat.netty.router;

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

    private String dataNodeName;
    private String statement;
    private int sqlType;

    // 是否强制走master
    private Boolean canRunSlave = true;


    public RouteResultsetNode(String dataNodeName, int sqlType, String statement){
        this.dataNodeName = dataNodeName;
        this.sqlType = sqlType;
        this.statement = statement;
    }





    @Override
    public int compare(RouteResultsetNode o1, RouteResultsetNode o2) {
        return 1;

    }
}
