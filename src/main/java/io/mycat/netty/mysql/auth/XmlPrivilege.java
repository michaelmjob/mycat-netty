package io.mycat.netty.mysql.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.*;
import java.util.*;

/**
 * Created by snow_young on 16/8/6.
 */
public class XmlPrivilege extends AbstractPrivilege implements Privilege, Source{
    private static final Logger logger = LoggerFactory.getLogger(XmlPrivilege.class);

    public XmlPrivilege(){
        super();
    }

    public void load() throws Exception {
        InputStream dtd = XmlPrivilege.class.getResourceAsStream("/user.dtd");
        InputStream xml = XmlPrivilege.class.getResourceAsStream("/user.xml");
        assert dtd != null;
        assert xml != null;
        // getDocument
        Element root = XmlUtils.getDocument(dtd, xml).getDocumentElement();

//        loadSystem(root);

        loadUsers(root);

        loadQuarantine(root);

        logger.info("XmlPrivilege users: {}", this.users);
        logger.info("XmlPrivilege whitehost: {}", this.whitehosts);
    }

    public void loadUsers(Element root) throws Exception{
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
                if(this.users.containsKey(name)){
                    throw new Exception(String.format("user %s difine again", name));
                }
                this.users.put(name, new UserConfig(password, schemas, Boolean.parseBoolean(readOnly)));
                if (schemas != null) {
                    String[] schemaListStr = schemas.split(",");
                    // ensure shcema exists
                }
            }
        }
    }

    public void loadQuarantine(Element root) {
        NodeList list = root.getElementsByTagName("host");
        // white hosts
        for(int i = 0; i < list.getLength(); i++){
            Node node = list.item(i);
            Element e = (Element) node;
            String host = e.getAttribute("host").trim();
            String user = e.getAttribute("user").trim();
            if(this.whitehosts.containsKey(host)){
                // error here
                logger.error("host:{} has existed!", host);
            }
            String[] users = user.split(",");
            // TODO: ensure user exists
            this.whitehosts.put(host, user);
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
}
