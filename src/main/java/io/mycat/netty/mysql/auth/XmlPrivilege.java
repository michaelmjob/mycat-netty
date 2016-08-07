package io.mycat.netty.mysql.auth;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by snow_young on 16/8/6.
 */
public class XmlPrivilege {
    private static final Logger logger = LoggerFactory.getLogger(XmlPrivilege.class);

    private static XmlPrivilege instance = new XmlPrivilege();

    public XmlPrivilege(){
        whitehosts = new HashMap<>();
        users = new HashMap<>();
    }
    // host -> userlist with ','
    private Map<String, String> whitehosts;
    // name -> password, schemas
    private Map<String, UserConfig> users;

    public static void load() throws Exception {
        InputStream dtd = XmlPrivilege.class.getResourceAsStream("/user.dtd");
        InputStream xml = XmlPrivilege.class.getResourceAsStream("/user.xml");
        assert dtd != null;
        assert xml != null;
        // getDocument
        Element root = getDocument(dtd, xml).getDocumentElement();

//        loadSystem(root);

        loadUsers(root);

        loadQuarantine(root);
        logger.info("XmlPrivilege users: {}", instance.users);
        logger.info("XmlPrivilege whitehost: {}", instance.whitehosts);
        // get whiteList and blackList

//        convert to Document
//        String fileName = "";
//        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//        factory.setNamespaceAware(false);
//        factory.setValidating(false);
//        DocumentBuilder builder = factory.newDocumentBuilder();
//        builder.setEntityResolver(new IgnoreDTDEntityResolver());
//        Document xmldoc = builder.parse(fileName);
//        Element whiteHost = (Element) xmldoc.getElementsByTagName("whitehost").item(0);
//        Element quarantine = (Element) xmldoc.getElementsByTagName("quarantine").item(0);
//
//        if(quarantine == null){
//            quarantine = xmldoc.createElement("quarantine");
//            Element root = xmldoc.getDocumentElement();
//            if(whiteHost == null){
//                whiteHost = xmldoc.createElement("host");
//                quarantine.appendChild(whiteHost);
//            }
//        }
    }


    public static void loadUsers(Element root) throws Exception{
        NodeList list = root.getElementsByTagName("user");
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                Map<String, String> props = loadproperties(e);
                String password = (String) props.get("password");
                String readOnly = (String) props.get("readOnly");
                String schemas = (String) props.get("schemas");
                if(instance.users.containsKey(name)){
                    throw new Exception(String.format("user %s difine again", name));
                }
                instance.users.put(name, new UserConfig(password, schemas, Boolean.parseBoolean(readOnly)));
                if (schemas != null) {
                    String[] schemaListStr = schemas.split(",");
                    // ensure shcema exists
                }
            }
        }
    }

    public static void loadQuarantine(Element root) {
        NodeList list = root.getElementsByTagName("host");

        // white hosts
        for(int i = 0; i < list.getLength(); i++){
            Node node = list.item(i);
            Element e = (Element) node;
            String host = e.getAttribute("host").trim();
            String user = e.getAttribute("user").trim();
            if(instance.whitehosts.containsKey(host)){
                // error here
            }
            String[] users = user.split(",");
            // TODO: ensure user exists
            instance.whitehosts.put(host, user);
        }
    }

    public static void loadSystem(Element root) {
        NodeList list = root.getElementsByTagName("system");
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            if (node instanceof Element) {

            }
        }


    }

    public static Map<String, String> loadproperties(Element parent) {
        Map<String, String> map = new HashMap<>();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getNodeName();
                assert "property".equals(name);
                String key = e.getAttribute("name");
                String value = e.getTextContent();
                map.put(key, value.trim());
            }
        }
        return map;
    }


    // common function, should extract to parent class
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserConfig{
        private String password;
        private String schemas;
        private boolean readOnly;
    }


}
