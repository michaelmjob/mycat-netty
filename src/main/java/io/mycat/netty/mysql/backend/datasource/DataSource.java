package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.mysql.backend.handler.ResponseHandler;
import io.mycat.netty.router.RouteResultsetNode;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by snow_young on 16/8/14.
 *
 * represent a real cluster,
 * consist of read nodes and write nodes, each node implementation is Host.
 */
@Data
public class DataSource {
    private static final Logger logger = LoggerFactory.getLogger(DataSource.class);

    public static final int BALANCE_NONE = 0;
    public static final int BALANCE_ALL_BACK = 1;
    public static final int BALANCE_ALL = 2;
    public static final int BALANCE_ALL_READ = 3;

    // not support many write

    protected Host writeHost;
    protected Host[] readHosts;

    private int balance;
    private Random random = new Random();

    private String[] schemas;
    private DataSourceConfig.DatanodeConfig datanodeConfig;
    private String hostname;

    public DataSource(String name, DataSourceConfig.DatanodeConfig datanodeConfig, String[] schemas){
        this.hostname = name;
        this.datanodeConfig = datanodeConfig;
        this.schemas = schemas;
    }


    public Collection<Host> getAllHosts(){
        List<Host> hosts = new ArrayList<>();
        hosts.addAll(Arrays.asList(readHosts));
        hosts.add(writeHost);
        return hosts;
    }

    /**
     * TODO: MOVE TO BALANCE STRATEGY
     *
     * 随机选择，按权重设置随机概率。
     * 在一个截面上碰撞的概率高，但调用量越大分布越均匀，而且按概率使用权重后也比较均匀，有利于动态调整提供者权重。
     *
     * */
    private Host randomSelect(ArrayList<Host> hosts){
        if(Objects.isNull(hosts) || hosts.isEmpty()){
            throw new RuntimeException("hosts is not illegal");
        }else{
            // should refactor here
            int length = hosts.size(); 	// 总个数
            int totalWeight = 0; 			// 总权重
            boolean sameWeight = true; 		// 权重是否都一样
            for (int i = 0; i < length; i++) {
                int weight = hosts.get(i).getHostConfig().getWeight();
                totalWeight += weight; 		// 累计总权重
                if (sameWeight && i > 0
                        && weight != hosts.get(i-1).getHostConfig().getWeight() ) {	  // 计算所有权重是否一样
                    sameWeight = false;
                }
            }

            if (totalWeight > 0 && !sameWeight ) {

                // 如果权重不相同且权重大于0则按总权重数随机
                int offset = random.nextInt(totalWeight);

                // 并确定随机值落在哪个片断上
                for (int i = 0; i < length; i++) {
                    offset -= hosts.get(i).getHostConfig().getWeight();
                    if (offset < 0) {
                        return hosts.get(i);
                    }
                }
            }

            // 如果权重相同或权重为0则均等随机
            return hosts.get( random.nextInt(length) );
        }
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
    }

    public void getConnection(String schema, boolean autoCommit, RouteResultsetNode node,
                              ResponseHandler responseHandler){
//        checkRequest(schema);
        logger.info("rrs runOnSlave {}", node.isCanRunInReadDB());
        if(node.isCanRunInReadDB()){
            // 不是写类型的操作
            // 添加balance
            Host host = getReadHosts()[0];
            try {
                host.getConnection(schema, autoCommit, responseHandler);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            // 写操作
            Host host = getWriteHost();
            try {
                host.getConnection(schema, autoCommit, responseHandler);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}

