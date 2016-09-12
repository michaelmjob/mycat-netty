package io.mycat.netty.config;

import io.mycat.netty.conf.XMLSchemaLoader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by snow_young on 16/8/7.
 * 测试数据库配置文件的加载
 */
public class schemaLoaderTest {
    private static final Logger logger = LoggerFactory.getLogger(schemaLoaderTest.class);

    @Test
    public void testXmlLoader() throws ParserConfigurationException, SAXException, IOException {

        XMLSchemaLoader xmlSchemaLoader = new XMLSchemaLoader();
//        new XMLSchemaLoader().load();
        xmlSchemaLoader.load();
        logger.info("reasStrategy : {}", xmlSchemaLoader.getDatasource().getDatanodes().get(0).getReadStrategy());


        // 提高测试覆盖率
    }
}
