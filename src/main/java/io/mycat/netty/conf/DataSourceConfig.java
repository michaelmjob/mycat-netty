package io.mycat.netty.conf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *  <datanode name="d0" balance="rr" maxconn="100" minconn="10" readtype="1" dbtype="mysql" dbdriver="builtin">
 *      <writehost url="localhost:3306" user="xujianhai" password="xujianhai"/>
 *      <readhost url="localhost:3306" user="xujianhai" password="xujianhai"/>
 *      <heartbeat>select user()</heartbeat>
 *  </datanode>
 *
 * Created by snow_young on 16/8/7.
 */
@Data
@AllArgsConstructor
public class DataSourceConfig {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceConfig.class);

    private List<DatanodeConfig> datanodes;

    public DataSourceConfig(){
        datanodes = new ArrayList<>();
    }

    //
    @Data
    @AllArgsConstructor
    public static class DatanodeConfig {
        private String name;
        //TODO: should enum
        private String balance;
        private int maxconn;
        private int minconn;
        // whether to read from master
        private boolean readtype;
        // should be enum
        private String dbtype;
        private String dbdriver;
        private HostConfig writehost;
        private List<HostConfig> readhost;
        private String readStrategy;

        public DatanodeConfig(){
            readhost = new ArrayList<>();
            writehost = new HostConfig();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HostConfig {
        private String url;
        private String user;
        private String password;
        private boolean readType;
        private int weight;
    }

}
