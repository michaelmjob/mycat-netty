package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.mysql.backend.NettyBackendSession;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by snow_young on 16/8/14.
 */
public class ConMap {
    // key : schema
    // ConQueue : 连接队列
    private final ConcurrentHashMap<String, ConQueue> items = new ConcurrentHashMap<String, ConQueue>();

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
            for (ConQueue queue2 : items.values()) {
                if (queue != queue2) {
                    con = tryTakeCon(queue2, autoCommit);
                    if (con != null) {
                        return con;
                    }
                }
            }
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
        return (int)items.get(host.getName()).getExecuteCount();
    }

    public void clearConnections(String reason, Host host) {

        items.clear();
    }
}
