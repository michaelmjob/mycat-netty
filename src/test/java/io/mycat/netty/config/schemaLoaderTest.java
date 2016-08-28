package io.mycat.netty.config;

import io.mycat.netty.conf.XMLSchemaLoader;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by snow_young on 16/8/7.
 * 测试数据库配置文件的加载
 */
public class schemaLoaderTest {

    @Test
    public void testXmlLoader() throws ParserConfigurationException, SAXException, IOException {
        new XMLSchemaLoader().load();

        // 提高测试覆盖率
    }
}
