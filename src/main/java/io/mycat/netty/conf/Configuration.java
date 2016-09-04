package io.mycat.netty.conf;

import io.mycat.netty.mysql.backend.SessionService;
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

    @Getter
    private static XMLSchemaLoader schemaLoader = new XMLSchemaLoader();

    private static SessionService sessionService = new SessionService();
    // name databaseName nodeConfiguration
    @Getter
    private static Map<String, DataSource> dataSources;

    public static void init() {
        // load database cluster definition
        try {
            logger.info("init schema.xml");
            schemaLoader.load();
        } catch (IOException | SAXException | ParserConfigurationException e) {
            logger.error("schema file load fail", e);
            System.exit(-1);
        }

        sessionService.init(schemaLoader);

    }

    public static Map<String, SchemaConfig> getSchemaCofnigs() {
        return schemaLoader.getSchemaConfigs();
    }
}