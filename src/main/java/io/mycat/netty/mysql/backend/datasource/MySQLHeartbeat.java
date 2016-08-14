package io.mycat.netty.mysql.backend.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by snow_young on 16/8/14.
 */
public class MySQLHeartbeat extends DBHeartbeat{
    private static final Logger logger = LoggerFactory.getLogger(MySQLHeartbeat.class);

//    mysqldetector
    private final Host host;

    private final ReentrantLock lock;

    public MySQLHeartbeat(Host host){
        this.host = host;
        this.lock = new ReentrantLock();
        this.status = INIT_STATUS;
//        this.heartbeatSql = host;
    }

}
