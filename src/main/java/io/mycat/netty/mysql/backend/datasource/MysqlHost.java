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

    public MysqlHost(String hostName, DataSourceConfig.HostConfig hostConfig, DataSourceConfig.DatanodeConfig dataNodeConfig,
                     boolean isReadNode) {
        super(hostName ,hostConfig, dataNodeConfig, isReadNode);
//        this.factory = new MySQLConnectionFactory();
    }

    // 需要放到connMap
    // 每一次操作结束的时候，需要放到conMap
    @Override
    public NettyBackendSession createNewConnection(String schema, boolean autocommit, ResponseHandler responseHandler) throws IOException {
        // 创建连接
        NettyBackendSession session = new NettyBackendSession();

        session.setAutocommit(autocommit);
        session.setResponseHandler(responseHandler);

        session.setPacketHeaderSize(SystemConfig.packetHeaderSize);
        session.setMaxPacketSize(SystemConfig.maxPacketSize);

        session.setUserName(this.getHostConfig().getUser());
        session.setPassword(this.getHostConfig().getPassword());

        String url = this.getHostConfig().getUrl();
        session.setHost(url.split(":")[0]);
//        set database name
        session.setCurrentDB(schema);
        session.setPort(Integer.parseInt(url.split(":")[1]));

        // blocking method
        // TODO: add async method
        //TODO: what if init failed, return null
        if(session.initConnect()) {
            logger.info("connect success");
            return session;
        }
        return null;
    }


    @Override
    public DBHeartbeat createHeartBeat() {
        return new MySQLHeartbeat(this);
    }
}
