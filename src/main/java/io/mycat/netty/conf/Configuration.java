package io.mycat.netty.conf;

import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Map;

/**
 * Created by snow_young on 16/8/10.
 * 配置中心
 */
public class Configuration {
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static XMLSchemaLoader schemaLoader = new XMLSchemaLoader();
    public static void init(){
        try {

            schemaLoader.load();
        } catch (IOException|SAXException|ParserConfigurationException e) {
            logger.error("schema file load fail", e);
            System.exit(-1);
        }
    }

    public static Map<String, SchemaConfig> getSchemaCofnigs(){
        return schemaLoader.getSchemaConfigs();
    }

    public static DataSource getDataSource(){
        return schemaLoader.getDatasource();
    }

}
