package io.mycat.netty.router;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by snow_young on 16/8/22.
 */
public class RouteStrategyFactory {
    private static RouteStrategy routeStrategy = null;

    private static ConcurrentHashMap<String, RouteStrategy> strategies = new ConcurrentHashMap<>();

    public void init(){
        strategies.put("druidparser", new DruidMycatRouteStrategy());

        routeStrategy = strategies.get("druidparser");
    }


    public RouteStrategy getRouteStrategy(){
        return routeStrategy;
    }

    public RouteStrategy getRouteStrategy(String type){
        return strategies.get(type);
    }
}
