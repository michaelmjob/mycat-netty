package io.mycat.netty.mysql.handler;

import io.mycat.netty.mysql.MysqlFrontendSession;

/**
 * Created by snow_young on 16/9/4.
 */
public class BlockingSyncMysqlSessionContext extends SyncMysqlSessionContext{

    private long timeout = 0l;

    public BlockingSyncMysqlSessionContext(MysqlFrontendSession frontSession, long timeout) {
        super(frontSession);
        this.timeout = timeout;
    }
}
