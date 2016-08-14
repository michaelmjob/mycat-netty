package io.mycat.netty.mysql.backend.datasource;

import lombok.Data;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by snow_young on 16/8/14.
 */
@Data
public abstract class DBHeartbeat {

    private static final long DEFAULT_HEARTBEAT_TIMEOUT = 30 * 1000L;
    private static final int DEFAULT_HEARTBEAT_RETRY = 10;

    protected static final int INIT_STATUS = 0;
    protected static final int OK_STATUS = 1;
    protected static final int ERROR_STATUS = -1;
    protected static final int TIMEOUT_STATUS = -2;

    protected long heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;
    protected long heartbeatRetry = DEFAULT_HEARTBEAT_RETRY;

    protected String heartbeatSql;

    protected int errorCount;
    // refactor: using byte
    protected volatile int status;
//    protected final AtomicBoolean isStop = new AtomicBoolean(true);
//    protected final AtomicBoolean isChecking = new AtomicBoolean(false);

//    sql recorder
//    protected final HeartbeatRecorder recorder = new HeartbeatRecorder();
//    protected final DataSourceSyncRecorder asynRecorder = new DataSourceSyncRecorder();




}
