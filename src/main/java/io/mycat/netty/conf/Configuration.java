package io.mycat.netty.conf;

import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.*;

/**
 * Created by snow_young on 16/8/10.
 * 配置中心
 */
public class Configuration {
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static XMLSchemaLoader schemaLoader = new XMLSchemaLoader();

    // name databaseName nodeConfiguration
    @Getter
    private static Map<String, DataSource> dataSources;

    public static void init(){
        // load database cluster definition
        try {
            logger.info("init schema.xml");
            schemaLoader.load();
        } catch (IOException|SAXException|ParserConfigurationException e) {
            logger.error("schema file load fail", e);
            System.exit(-1);
        }


        // init
        logger.info("load datasource");
//        too much time wasted in init
        load_datasource();
        logger.info("init datasource");
        init_datasource();
    }

    public static Map<String, SchemaConfig> getSchemaCofnigs(){
        return schemaLoader.getSchemaConfigs();
    }

//    public static DataSourceConfig getDataSourceConfigs(){
//        return schemaLoader.getDatasource();
//    }

    // each schema on each node only init once;
    private static Map<String, Set<String>> getNode2DBMap_distinct(){
        Map<String, Set<String>> node2dbMap = new HashMap<>();
        for(SchemaConfig schemaConfig : schemaLoader.getSchemaConfigs().values()) {
            for (TableConfig tableConfig : schemaConfig.getTables().values()) {
                for(TableConfig.NodeConfig nodeConfig : tableConfig.getDatasource()){
                    if(!node2dbMap.containsKey(nodeConfig.getDatanode())){
                        node2dbMap.put(nodeConfig.getDatanode(), new HashSet<String>());
                    }
                    node2dbMap.get(nodeConfig.getDatanode()).add(nodeConfig.getDatabase());
                }
            }
        }
        return node2dbMap;
    }

    // each schema on each node only init many times;
    private static Map<String, List<String>> getNode2DBMap_repeat(){
        Map<String, List<String>> node2dbMap = new HashMap<>();
        for(SchemaConfig schemaConfig : schemaLoader.getSchemaConfigs().values()) {
            for (TableConfig tableConfig : schemaConfig.getTables().values()) {
                for(TableConfig.NodeConfig nodeConfig : tableConfig.getDatasource()){
                    if(!node2dbMap.containsKey(nodeConfig.getDatanode())){
                        node2dbMap.put(nodeConfig.getDatanode(), new ArrayList<String>());
                    }
                    node2dbMap.get(nodeConfig.getDatanode()).add(nodeConfig.getDatabase());
                }
            }
        }
        return node2dbMap;
    }

    private static void load_datasource(){
        load_datasource_repeat();
    }

    public static void load_datasource_repeat(){
        dataSources = new HashMap<>();

        Map<String, List<String>> node2dbMapping = getNode2DBMap_repeat();
        DataSourceConfig dataSourceConfig = schemaLoader.getDatasource();
        for(DataSourceConfig.DatanodeConfig datanodeConfig :  dataSourceConfig.getDatanodes()){
            DataSource dataSource = new MysqlDataSource(datanodeConfig.getName(),
                    datanodeConfig,
                    node2dbMapping.get(datanodeConfig.getName()).toArray(new String[]{}));

            dataSources.put(datanodeConfig.getName(), dataSource);
        }
    }

    private static void load_datasource_distinct(){
        dataSources = new HashMap<>();

        Map<String, Set<String>> node2dbMapping = getNode2DBMap_distinct();
        DataSourceConfig dataSourceConfig = schemaLoader.getDatasource();
        for(DataSourceConfig.DatanodeConfig datanodeConfig :  dataSourceConfig.getDatanodes()){
            DataSource dataSource = new MysqlDataSource(datanodeConfig.getName(),
                    datanodeConfig,
                    node2dbMapping.get(datanodeConfig.getName()).toArray(new String[]{}));

            dataSources.put(datanodeConfig.getName(), dataSource);
        }
    }

    private static void init_datasource(){
        for(DataSource dataSource : dataSources.values()){
            dataSource.init();
        }
    }
}