package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.handler.ConnectionHeartBeatHandler;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.packet.ErrorPacket;
import io.mycat.netty.mysql.packet.OkPacket;
import io.mycat.netty.mysql.response.ResultSetPacket;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by snow_young on 16/8/14.
 *
 * represent a real node providing many database operation,
 * for read or write, u should know : one node may have many databases;
 */
// TODO: ADD HEARTBEAT ACTION
@Data
public abstract class Host {
    private static final Logger logger = LoggerFactory.getLogger(Host.class);

    // schema name,
    private String name;
//    private String dbname;

    // conMap : used in other
    private ConMap conMap = new ConMap();

    private DBHeartbeat heartbeat;
    private final ConnectionHeartBeatHandler connectionHeartBeatHandler = new ConnectionHeartBeatHandler();

    private volatile long heartbeatRecoveryTime;

//    private final boolean readNode;
//  private volatile long heatbeatRecoveryTime;

    private AtomicLong readCount = new AtomicLong(0);
    private AtomicLong writeCount = new AtomicLong(0);

    DataSourceConfig.HostConfig hostConfig;
    DataSourceConfig.DatanodeConfig datanodeConfig;

    public Host(DataSourceConfig.HostConfig hostConfig, DataSourceConfig.DatanodeConfig dataNodeConfig,
                boolean isReadNode){
        this.hostConfig = hostConfig;
        this.datanodeConfig = dataNodeConfig;
        heartbeat = this.createHeartBeat();
        this.name = hostConfig.getUrl();
//        this.dbname = dbname;
    }

    public void init(String dbname) throws InterruptedException {
        CountDownLatch count = new CountDownLatch(this.getDatanodeConfig().getMinconn());
        for(int i = 0 ; i < this.getDatanodeConfig().getMinconn(); i++){
//             create connection
            try {
                createNewConnection(dbname, true, new ResponseHandler() {
                    @Override
                    public void errorResponse(ErrorPacket packet, NettyBackendSession session) {
                        logger.error("error response with initializing");
                        System.exit(-1);
                    }

                    @Override
                    public void okResponse(OkPacket packet, NettyBackendSession session) {
                        // 默认初始化的时候放入 自动提交的队列
                        Host.this.conMap.getSchemaConQueue(dbname).getConnQueue(true).add(session);
                        count.countDown();
                    }

                    @Override
                    public void resultsetResponse(ResultSetPacket resultSetPacket, NettyBackendSession session) {
                        logger.error("should not happen here");
                        System.exit(-1);
                    }
                });
            } catch (IOException e) {
                logger.error("init error", e);
                System.exit(-1);
            }
        }
        count.await();
    }

    public void getConnection(String schema, boolean autocommit,
                              ResponseHandler handler)
            throws IOException {
        // conMap 是记录所有数据库的连接
        NettyBackendSession con = this.conMap.tryTakeCon(schema, autocommit);
        if (con != null) {
            markConTaken(con, handler, schema);
            return;
        } else {
            int activeCons = this.getActiveCount();// 当前最大活动连接
            if (activeCons + 1 > this.getDatanodeConfig().getMaxconn()) {// 下一个连接大于最大连接数
                logger.error("the max activeConnnections size can not be max than maxconnections");
                throw new IOException(
                        "the max activeConnnections size can not be max than maxconnections");
            } else { // create connection
                logger.info("no idle connection in pool,create new connection for "
                        + this.name + " of schema " + schema);
                // should add connection get handler
                //
                createNewConnection(schema, autocommit, handler);
            }
        }
    }

    public int getActiveCount() {
        return this.conMap.getActiveCount4Host(this);
    }

    private NettyBackendSession markConTaken(NettyBackendSession conn,
                                      final ResponseHandler handler,
                                        String schema) {

        conn.setBorrowed(true);
        if (!conn.getCurrentDB().equals(schema)) {
            // need do schema syn in before sql send
            conn.setCurrentDB(schema);
        }
        ConQueue queue = conMap.getSchemaConQueue(schema);
        queue.incExecuteCount();
        conn.setLastTime(System.currentTimeMillis()); // 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致sql执行失败
//        handler.connectionAcquired(conn);
        conn.setResponseHandler(handler);
        return conn;
    }


    public abstract void createNewConnection(String dbname, boolean autoCommit, ResponseHandler handler) throws IOException;

    public abstract DBHeartbeat createHeartBeat();
}
