package io.mycat.netty.conf;

import io.mycat.netty.mysql.backend.SessionService;
import io.mycat.netty.mysql.backend.datasource.DataSource;
import io.mycat.netty.mysql.backend.datasource.MysqlDataSource;
import io.mycat.netty.router.RouteStrategyFactory;
import lombok.Getter;
import lombok.Setter;
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

    // just 4 test
    @Setter
    private static XMLSchemaLoader schemaLoader = new XMLSchemaLoader();

    @Getter
    private static SessionService sessionService = new SessionService();
    // name databaseName nodeConfiguration

    public static void init() {
        // load database cluster definition
        try {
            logger.info("init schema.xml");
            schemaLoader.load();
        } catch (IOException | SAXException | ParserConfigurationException e) {
            logger.error("schema file load fail", e);
            System.exit(-1);
        }

        // should move 2 startUp
        sessionService.init(schemaLoader);

        RouteStrategyFactory.init();

    }

    public static Map<String, SchemaConfig> getSchemaCofnigs() {
        return schemaLoader.getSchemaConfigs();
    }
}