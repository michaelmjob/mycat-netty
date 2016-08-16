package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.conf.SystemConfig;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by snow_young on 16/8/14.
 */
public class MysqlHost extends Host{
    private static final Logger logger = LoggerFactory.getLogger(MysqlHost.class);

//    private final MySQLConnectionFactory factory;

    public MysqlHost(DataSourceConfig.HostConfig hostConfig, DataSourceConfig.DatanodeConfig dataNodeConfig,
                     boolean isReadNode) {
        super(hostConfig, dataNodeConfig, isReadNode);
//        this.factory = new MySQLConnectionFactory();
    }

    // 需要放到connMap
    // 每一次操作结束的时候，需要放到conMap
    @Override
    public void createNewConnection(String schema, boolean autocommit, ResponseHandler handler) throws IOException {
        // 创建连接
        NettyBackendSession session = new NettyBackendSession();

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);

        session.setUserName(this.getHostConfig().getUser());
        session.setPassword(this.getHostConfig().getPassword());

        String url = this.getHostConfig().getUrl();
        session.setHost(url.split(":")[0]);
//        set database name
        session.setCurrentDB(schema);
        session.setPort(Integer.parseInt(url.split(":")[1]));
        session.setResponseHandler(handler);

        // blocking method
        // TODO: add async method
        session.initConnect();
        logger.info("connect success");
    }

//    @Override
//    public void createNewConnection(ResponseHandler handler,String schema) throws IOException {
//        factory.make(this, handler,schema);
//    }

    @Override
    public DBHeartbeat createHeartBeat() {
        return new MySQLHeartbeat(this);
    }
}
