package io.mycat.netty.mysql.backend.datasource;

import io.mycat.netty.conf.DataSourceConfig;

/**
 * Created by snow_young on 16/8/14.
 */
public class MysqlDataSource extends DataSource{

    public MysqlDataSource(String name, DataSourceConfig.DatanodeConfig datanodeConfig, String[] schemas) {
        super(name, datanodeConfig, schemas);

        readHosts = new MysqlHost[datanodeConfig.getReadhost().size()];
        for(int i = 0; i < datanodeConfig.getReadhost().size(); i++){
            readHosts[i] = new MysqlHost(datanodeConfig.getReadhost().get(i), datanodeConfig, true);
        }
        writeHost = new MysqlHost(datanodeConfig.getWritehost(), datanodeConfig, false);
    }

}
