package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.conf.DataSourceConfig;

/**
 * Created by snow_young on 16/8/14.
 */
public class MysqlDataSource extends DataSource{

    public MysqlDataSource(String hostname, DataSourceConfig.DatanodeConfig datanodeConfig, String[] schemas) {
        super(hostname, datanodeConfig, schemas);

        readHosts = new MysqlHost[datanodeConfig.getReadhost().size()];
        for(int i = 0; i < datanodeConfig.getReadhost().size(); i++){
            String hostName = datanodeConfig.getName() + "_read_" + String.valueOf(i);
            readHosts[i] = new MysqlHost(hostName, datanodeConfig.getReadhost().get(i), datanodeConfig, true);
        }
        String hostName = datanodeConfig.getName() + "_write";
        writeHost = new MysqlHost(hostName, datanodeConfig.getWritehost(), datanodeConfig, false);
    }

}
