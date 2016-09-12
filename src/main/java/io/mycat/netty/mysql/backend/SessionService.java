package io.mycat.netty.mysql.backend;

import io.mycat.netty.conf.*;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import io.mycat.netty.mysql.backend.strategy.LeastConnStrategy;
import io.mycat.netty.mysql.backend.strategy.ReadStrategy;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.validation.Schema;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Created by snow_young on 16/8/29.
 * should all be static
 */
public class SessionService implements Closeable{
    private static final Logger logger = LoggerFactory.getLogger(SessionService.class);

    @Getter
    private static Map<String, DataSource> dataSources;

    private ReadStrategy readStrategy;

    public void init(XMLSchemaLoader schemaLoader) {
        logger.info("load datasource from cconfig");
        load_datasource(schemaLoader.getDatasource(), schemaLoader.getSchemaConfigs().values());
        logger.info("init datasource");
        init_datasource();

    }

    public static Host getSession(String datanode, boolean readOnly, String database) {
        DataSource dataSource = dataSources.get(datanode);
        Host host;
        // TODO: 使用最少channel算法选取read, 需要 一个queue store session to send
        if (readOnly) {
            host = dataSource.getOneReadHost(database);
        } else {
            host = dataSource.getWriteHost();
        }
        return host;
    }

    // 先采用同步session
//    public void newSession() {
//
//    }

    // ========== init ===============

    public void load_datasource(DataSourceConfig dataSourceConfig, Collection<SchemaConfig> schemaConfigs) {
        load_datasource_repeat(dataSourceConfig, schemaConfigs);
    }

    public void load_datasource_repeat(DataSourceConfig dataSourceConfig, Collection<SchemaConfig> schemaConfigs) {
        dataSources = new HashMap<>();

        Map<String, List<String>> node2dbMapping = getNode2DBMap_repeat(schemaConfigs);
        for (DataSourceConfig.DatanodeConfig datanodeConfig : dataSourceConfig.getDatanodes()) {
            DataSource dataSource = new MysqlDataSource(datanodeConfig.getName(),
                    datanodeConfig,
                    node2dbMapping.get(datanodeConfig.getName()).toArray(new String[]{}));

            dataSources.put(datanodeConfig.getName(), dataSource);
        }
    }

    private void load_datasource_distinct(DataSourceConfig dataSourceConfig, Collection<SchemaConfig> schemaConfigs) {
        dataSources = new HashMap<>();

        Map<String, Set<String>> node2dbMapping = getNode2DBMap_distinct(schemaConfigs);
        for (DataSourceConfig.DatanodeConfig datanodeConfig : dataSourceConfig.getDatanodes()) {
            DataSource dataSource = new MysqlDataSource(datanodeConfig.getName(),
                    datanodeConfig,
                    node2dbMapping.get(datanodeConfig.getName()).toArray(new String[]{}));

            dataSources.put(datanodeConfig.getName(), dataSource);
        }
    }

    public void init_datasource() {
        for (DataSource dataSource : dataSources.values()) {
            dataSource.init();
            // class reflection
        }
    }


    // =========== util ===============
    // each schema on each node only init once;
    private Map<String, Set<String>> getNode2DBMap_distinct(Collection<SchemaConfig> schemaConfigs) {
        Map<String, Set<String>> node2dbMap = new HashMap<>();
        for (SchemaConfig schemaConfig : schemaConfigs) {
            for (TableConfig tableConfig : schemaConfig.getTables().values()) {
                for (TableConfig.NodeConfig nodeConfig : tableConfig.getDatasource()) {
                    if (!node2dbMap.containsKey(nodeConfig.getDatanode())) {
                        node2dbMap.put(nodeConfig.getDatanode(), new HashSet<String>());
                    }
                    node2dbMap.get(nodeConfig.getDatanode()).add(nodeConfig.getDatabase());
                }
            }
        }
        return node2dbMap;
    }

    // each schema on each node only init many times;
    private Map<String, List<String>> getNode2DBMap_repeat(Collection<SchemaConfig> schemaConfigs) {
        Map<String, List<String>> node2dbMap = new HashMap<>();
        for (SchemaConfig schemaConfig : schemaConfigs) {
            for (TableConfig tableConfig : schemaConfig.getTables().values()) {
                for (TableConfig.NodeConfig nodeConfig : tableConfig.getDatasource()) {
                    if (!node2dbMap.containsKey(nodeConfig.getDatanode())) {
                        node2dbMap.put(nodeConfig.getDatanode(), new ArrayList<String>());
                    }
                    node2dbMap.get(nodeConfig.getDatanode()).add(nodeConfig.getDatabase());
                }
            }
        }
        return node2dbMap;
    }

    @Override
    public void close() throws IOException {
        dataSources.values().forEach(dataSource -> {
            try {
                dataSource.close();
            } catch (IOException e) {
                logger.error("should not occur here, error: {}", e);
            }
        });
    }
}
