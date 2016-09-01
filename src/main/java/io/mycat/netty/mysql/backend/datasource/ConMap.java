package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.mysql.backend.NettyBackendSession;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by snow_young on 16/8/14.
 * 是所有数据库的连接都在这里 ?
 */
public class ConMap {
    // key : schema
    // ConQueue : 连接队列
    // 以前这个走的是  host, ConQueue
    private final ConcurrentHashMap<String, ConQueue> items = new ConcurrentHashMap<String, ConQueue>();
    private final Map<String, AtomicInteger> hostsActiveCount = new ConcurrentHashMap<>();

    public ConQueue getSchemaConQueue(String schema) {
        ConQueue queue = items.get(schema);
        if (queue == null) {
            ConQueue newQueue = new ConQueue();
            queue = items.putIfAbsent(schema, newQueue);
            return (queue == null) ? newQueue : queue;
        }
        return queue;
    }



    public NettyBackendSession tryTakeCon(final String schema, boolean autoCommit) {
        final ConQueue queue = items.get(schema);
        NettyBackendSession con = tryTakeCon(queue, autoCommit);
        if (con != null) {
            return con;
        } else {
//            这样无法保证后端 Host 对象的 databaseName
//            for (ConQueue queue2 : items.values()) {
//                if (queue != queue2) {
//                    con = tryTakeCon(queue2, autoCommit);
//                    if (con != null) {
//                        return con;
//                    }
//                }
//            }
        }
        return null;
    }

    private NettyBackendSession tryTakeCon(ConQueue queue, boolean autoCommit) {
        NettyBackendSession con = null;
        if (queue != null && ((con = queue.takeIdleCon(autoCommit)) != null)) {
            return con;
        } else {
            return null;
        }

    }

    public Collection<ConQueue> getAllConQueue() {
        return items.values();
    }

    // 分了JDBC MysqlConnection
    // TODO : ADD JDBC CONNECTION
    public int getActiveCountForSchema(String schema, Host host, boolean autoCommit) {
        int total = 0;

        for(NettyBackendSession session : items.get(schema).getConnQueue(autoCommit)){
//            if(session.getHost().equals(host))
        }
        return total;
    }

    public int getActiveCount4Host(Host host) {
        
        return hostsActiveCount.get(host.getName()).get();
//        return (int)items.get(host.getName()).getExecuteCount();
    }

    public void decreaseActiveCount4Host(Host host){
        this.hostsActiveCount.get(host.getName()).decrementAndGet();
    }

    public void addActiveCount4Host(Host host){
        this.hostsActiveCount.get(host.getName()).addAndGet(1) ;
    }

    public void clearConnections(String reason, Host host) {

        items.clear();
    }
}
