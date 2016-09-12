package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.mysql.backend.NettyBackendSession;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.mysql.backend.strategy.LeastConnStrategy;
import io.mycat.netty.mysql.backend.strategy.ReadStrategy;
import io.mycat.netty.mysql.backend.strategy.ReadStrategyFactory;
import io.mycat.netty.router.RouteResultsetNode;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Created by snow_young on 16/8/14.
 *
 * represent a real cluster,
 * consist of read nodes and write nodes, each node implementation is Host.
 */
@Data
public class DataSource implements Closeable{
    private static final Logger logger = LoggerFactory.getLogger(DataSource.class);

    public static final int BALANCE_NONE = 0;
    public static final int BALANCE_ALL_BACK = 1;
    public static final int BALANCE_ALL = 2;
    public static final int BALANCE_ALL_READ = 3;

    private ReadStrategy readStrategy;
    // not support many write

    protected Host writeHost;
    protected Host[] readHosts;

    private int balance;
    private Random random = new Random();

    private String[] schemas;
    private DataSourceConfig.DatanodeConfig datanodeConfig;
    private String hostname;

    public DataSource(String hostname, DataSourceConfig.DatanodeConfig datanodeConfig, String[] schemas){
        this.hostname = hostname;
        this.datanodeConfig = datanodeConfig;
        this.schemas = schemas;
    }


    public Collection<Host> getAllHosts(){
        List<Host> hosts = new ArrayList<>();
        hosts.addAll(Arrays.asList(readHosts));
        hosts.add(writeHost);
        return hosts;
    }

    public void init(){
        for(int i = 0; i < readHosts.length; i++){
            for(int j = 0 ; j < schemas.length; j++){
                try {
                    readHosts[i].init(schemas[j]);
                } catch (InterruptedException e) {
                    logger.error("readhost[{}] init session for schema[{}] failed", readHosts[i], schemas[j]);
                    System.exit(-1);
                }
            }
        }
        for(int j = 0 ; j < schemas.length; j++){
            try {
                writeHost.init(schemas[j]);
            } catch (InterruptedException e) {
                logger.error("writehost[{}] init session for schema[{}] failed", writeHost, schemas[j]);
                System.exit(-1);
            }
        }

        // should use class reflection
        readStrategy = ReadStrategyFactory.buildStrategy(datanodeConfig.getReadStrategy(), readHosts);
    }

    public Host getOneReadHost(String database){
        return readStrategy.select(database);
    }

//    public NettyBackendSession getConnection(String schema, boolean autoCommit, RouteResultsetNode node,
//                              ResponseHandler responseHandler){
////        checkRequest(schema);
//        logger.info("rrs runOnSlave {}", node.isCanRunInReadDB());
//        if(node.isCanRunInReadDB()){
//            // 不是写类型的操作
//            // TODO: 添加balance
////            Host host = getReadHosts()[0];
//            Host host = readStrategy.select(schema);
//            try {
//                return host.getConnection(schema, autoCommit);
////                return host.getConnection(schema, autoCommit, responseHandler);
//            } catch (IOException e) {
//                logger.error("get connection for schema[{}] with autocommit[{}] failed", schema, autoCommit, e);
//                return null;
//            }
//        }else{
//            // 写操作
//            Host host = getWriteHost();
//            try {
//                return host.getConnection(schema, autoCommit);
////                return host.getConnection(schema, autoCommit, responseHandler);
//            } catch (IOException e) {
//                logger.error("get connection for schema[{}] with autocommit[{}] failed", schema, autoCommit, e);
//                return null;
//            }
//        }
//
//    }

    @Override
    public void close() throws IOException {
        writeHost.close();
        for(Host host : readHosts){
            host.close();
        }
    }
}

