package io.mycat.netty.router;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Created by snow_young on 16/8/12.
 */
@Data
public class RouteResultset implements Serializable{
    private String statement;
//    private int sqlType;
    private RouteResultsetNode[] nodes;

    private boolean canRunSlave = false;
    private final int sqlType;
    private String primaryKey;
    private int limitStart;
    private int limitSize;
    private boolean autocommit = true;

    public RouteResultset(String statement, int sqlType){
        this.statement = statement;
        this.sqlType = sqlType;
    }



}
