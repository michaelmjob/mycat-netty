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

    public RouteResultset(String statement){
        this.statement = statement;
    }


}
