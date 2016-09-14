package io.mycat.netty.mysql.backend.datasoource;

import io.mycat.netty.conf.DataSourceConfig;
import io.mycat.netty.TestConstants;
import io.mycat.netty.util.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by snow_young on 16/9/9.
 */
public class CommonTest {
    protected static DataSourceConfig.HostConfig hostConfig;
    protected static DataSourceConfig.HostConfig writeConfig;
    protected static DataSourceConfig.DatanodeConfig datanodeConfig;
    
    public void init(){
        hostConfig = new DataSourceConfig.HostConfig(TestConstants.db0url, TestConstants.user, TestConstants.pass, true, 1);
        List<DataSourceConfig.HostConfig> readHosts = new ArrayList<>();
        readHosts.add(hostConfig);

        writeConfig = new DataSourceConfig.HostConfig(TestConstants.db0url, TestConstants.user, TestConstants.pass, false, 1);
        datanodeConfig = new DataSourceConfig.DatanodeConfig(TestConstants.DB0, "blns", 100, 10, true, "mysql",
                "builtin", writeConfig, readHosts, Constants.READ_STRATEGY);
    }
}
