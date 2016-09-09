package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.mysql.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by snow_young on 16/9/9.
 */
public class CommonTest {
    protected DataSourceConfig.HostConfig hostConfig;
    protected DataSourceConfig.HostConfig writeConfig;
    protected DataSourceConfig.DatanodeConfig datanodeConfig;
    
    public void init(){
        hostConfig = new DataSourceConfig.HostConfig(Constants.db0url, Constants.user, Constants.pass, true, 1);
        List<DataSourceConfig.HostConfig> readHosts = new ArrayList<>();
        readHosts.add(hostConfig);

        writeConfig = new DataSourceConfig.HostConfig(Constants.db0url, Constants.user, Constants.pass, false, 1);
        datanodeConfig = new DataSourceConfig.DatanodeConfig(Constants.DB0, "blns", 100, 10, true, "mysql",
                "builtin", writeConfig, readHosts);
    }
}
