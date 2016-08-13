package io.mycat.netty.router;

import java.io.Serializable;

/**
 * Created by snow_young on 16/8/12.
 */
public class RouteResultset implements Serializable{
    private String statement;
    private int sqlType;
    private RouteResultsetNode[] nodes;

    private boolean canRunSlave = false;

    public RouteResultset(String statement, int sqlType){
        this.statement = statement;
        this.sqlType = sqlType;
    }


}
