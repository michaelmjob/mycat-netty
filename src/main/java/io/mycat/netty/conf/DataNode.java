package io.mycat.netty.conf;

import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.router.RouteResultsetNode;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by snow_young on 16/8/14.
 *
 * should remove this class, maybe extra
 *
 * read && write hosts consist of datasource, one datanode owns a datanode with specified database name,
 * so, if database name is different, differs te datanode
 */
@Data
public class DataNode {
    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

    protected String name;
    protected String database;
    protected DataSource dataSource;

//    private void checkRequest(String schema){
//        if (schema != null
//                && !schema.equals(this.database)) {
//            throw new RuntimeException(
//                    "invalid param ,connection request db is :"
//                            + schema + " and datanode db is "
//                            + this.database);
//        }
//    }

    // without attachment!
    public void getConnection(String schema, boolean autoCommit, RouteResultsetNode node,
                              ResponseHandler responseHandler){
//        checkRequest(schema);
        logger.info("rrs runOnSlave {}", node.isCanRunInReadDB());
        if(node.isCanRunInReadDB()){
            // 不是写类型的操作
            // 添加balance
            Host host = dataSource.getReadHosts()[0];
            try {
//                host.getConnection(schema, autoCommit, responseHandler);
                host.getConnection(schema, autoCommit);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            // 写操作
            Host host = dataSource.getWriteHost();
            try {
//                host.getConnection(schema, autoCommit, responseHandler);
                host.getConnection(schema, autoCommit);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
