package io.mycat.netty.mysql.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by snow_young on 16/8/7.
 */
public class XmlUtils {
    private static final Logger logger = LoggerFactory.getLogger(XmlUtils.class);

    // get Document object from xml with dtd validation
    public static Document getDocument(final InputStream dtd, InputStream xml) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true);
        factory.setNamespaceAware(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        // 处理xml文件中的entity读取
        builder.setEntityResolver((publicId, systemId) -> {
            return new InputSource(dtd);
        });
        builder.setErrorHandler(new ErrorHandler() {
            @Override
            public void warning(SAXParseException exception) throws SAXException {
                logger.error("parse warnging :  {}", exception);
            }

            @Override
            public void error(SAXParseException exception) throws SAXException {
                logger.error("parse error :  {}", exception);
            }

            @Override
            public void fatalError(SAXParseException exception) throws SAXException {
                logger.error("parse fatal error :  {}", exception);
            }
        });

        return builder.parse(xml);
    }
}
