package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.mysql.backend.NettyBackendSession;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by snow_young on 16/8/14.
 *
 * 自动提交 手动提交两个队列
 *
 * merge two queue to implement queue interface
 */
public class ConQueue {
    private final ConcurrentLinkedQueue<NettyBackendSession> autoCommitCons = new ConcurrentLinkedQueue<NettyBackendSession>();
    private final ConcurrentLinkedQueue<NettyBackendSession> manCommitCons = new ConcurrentLinkedQueue<NettyBackendSession>();
    private AtomicLong executeCount = new AtomicLong(0);

    public NettyBackendSession takeIdleCon(boolean autoCommit) {
        ConcurrentLinkedQueue<NettyBackendSession> f1 = autoCommitCons;
        ConcurrentLinkedQueue<NettyBackendSession> f2 = manCommitCons;

        if (!autoCommit) {
            f1 = manCommitCons;
            f2 = autoCommitCons;

        }
        NettyBackendSession con = f1.poll();
        if (con == null || con.isClosedOrQuit()) {
            con = f2.poll();
        }
        if (con == null || con.isClosedOrQuit()) {
            return null;
        } else {
            return con;
        }

    }

    public long getExecuteCount() {
        return executeCount.get();
    }

    public void incExecuteCount() {
        this.executeCount.incrementAndGet();
    }

    public void decExecuteCount() {
        this.executeCount.decrementAndGet();
    }

    public void removeCon(NettyBackendSession con) {
        if (!autoCommitCons.remove(con)) {
            manCommitCons.remove(con);
        }
    }

    public boolean isSameCon(NettyBackendSession con) {
        if (autoCommitCons.contains(con)) {
            return true;
        } else if (manCommitCons.contains(con)) {
            return true;
        }
        return false;
    }

    public ConcurrentLinkedQueue<NettyBackendSession> getAutoCommitCons() {
        return autoCommitCons;
    }

    public ConcurrentLinkedQueue<NettyBackendSession> getManCommitCons() {
        return manCommitCons;
    }

    public ConcurrentLinkedQueue<NettyBackendSession> getConnQueue(boolean autocimmit){
        if(autocimmit){
            return getAutoCommitCons();
        }else{
            return getManCommitCons();
        }
    }

    public ArrayList<NettyBackendSession> getIdleConsToClose(int count) {
        ArrayList<NettyBackendSession> readyCloseCons = new ArrayList<NettyBackendSession>(
                count);
        while (!manCommitCons.isEmpty() && readyCloseCons.size() < count) {
            NettyBackendSession theCon = manCommitCons.poll();
            if (theCon != null) {
                readyCloseCons.add(theCon);
            }
        }
        while (!autoCommitCons.isEmpty() && readyCloseCons.size() < count) {
            NettyBackendSession theCon = autoCommitCons.poll();
            if (theCon != null) {
                readyCloseCons.add(theCon);
            }

        }
        return readyCloseCons;
    }
}
