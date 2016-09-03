package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.mysql.backend.NettyBackendSession;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

    // sessionIdTag for each session created on proxy
    private AtomicLong sessionIdTag = new AtomicLong(0l);

    public ConQueue getSchemaConQueue(String schema) {
        ConQueue queue = items.get(schema);
        if (queue == null) {
            ConQueue newQueue = new ConQueue();
            queue = items.putIfAbsent(schema, newQueue);
            return (queue == null) ? newQueue : queue;
        }
        return queue;
    }


    public NettyBackendSession tryTakeCon(final String host, final String schema, boolean autoCommit) {
        final ConQueue queue = items.get(schema);
        NettyBackendSession con = tryTakeCon(queue, autoCommit);
        if (con != null) {
            hostsActiveCount.get(host).getAndIncrement();
            queue.incExecuteCount();
            return con;
        }
        return null;
    }

    private NettyBackendSession tryTakeCon(ConQueue queue, boolean autoCommit) {
        NettyBackendSession con = null;
        if (queue != null && ((con = queue.takeIdleCon(autoCommit)) != null)) {
            return con;
        }
        return null;
    }

    public Collection<ConQueue> getAllConQueue() {
        return items.values();
    }

    // 分了JDBC MysqlConnection
    // TODO : ADD JDBC CONNECTION
    public int getActiveCountForSchema(String schema, Host host, boolean autoCommit) {
        int total = 0;

        for (NettyBackendSession session : items.get(schema).getConnQueue(autoCommit)) {
//            if(session.getHost().equals(host))
        }
        return total;
    }

    public int getActiveCount4Host(Host host) {
        // TODO: init && null solve here
        return hostsActiveCount.get(host.getName()).get();
    }

    public void put(String host, String db, NettyBackendSession session) {
        session.setSessionId(sessionIdTag.getAndIncrement());
        getSchemaConQueue(db).getConnQueue(session.isAutocommit()).add(session);
        hostsActiveCount.put(host, new AtomicInteger());
    }

    public void back(NettyBackendSession session) {
        getSchemaConQueue(session.getCurrentDB()).back(session);
        // owner is null
        hostsActiveCount.get(session.getOwner().getName()).decrementAndGet();
    }

    public void decreaseActiveCount4Host(Host host) {
        this.hostsActiveCount.get(host.getName()).decrementAndGet();
    }

    public void addActiveCount4Host(Host host) {
        this.hostsActiveCount.get(host.getName()).addAndGet(1);
    }

    public void clearConnections(String reason, Host host) {
        items.clear();
    }
}
