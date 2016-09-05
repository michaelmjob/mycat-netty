package io.mycat.netty.conf;

import io.mycat.netty.mysql.auth.XmlUtils;
import io.mycat.netty.router.partition.AbstractPartition;
import io.mycat.netty.router.partition.Partition;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.swing.text.html.parser.DTD;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by snow_young on 16/8/7.
 */
@Data
public class XMLSchemaLoader {
    private static final Logger logger = LoggerFactory.getLogger(XMLSchemaLoader.class);

    private static final String DTDFILE = "/schema.dtd";
    private static final String SCHEMAFILE = "/schema.xml";

    private String dtdFile = DTDFILE;
    private String schemaFile = SCHEMAFILE;

    private Map<String, SchemaConfig> schemaConfigs;
    private DataSourceConfig datasource;

    public XMLSchemaLoader() {
        schemaConfigs = new HashMap<>();
        datasource = new DataSourceConfig();
    }

    public void load() throws IOException, SAXException, ParserConfigurationException {
        InputStream dtd = XMLSchemaLoader.class.getResourceAsStream(dtdFile);
        InputStream xml = XMLSchemaLoader.class.getResourceAsStream(schemaFile);
        assert dtd != null;
        assert xml != null;

        Element root = XmlUtils.getDocument(dtd, xml).getDocumentElement();

        loadDatasource(root);
        loadSchema(root);

        logger.info("===================================================================");
        logger.info("===================================================================");
        logger.info("instance : {}", this);

        logger.info("===================================================================");
    }


    public PartitionConfig getPartition(Element root) {
        Element partitionNode = (Element) root.getElementsByTagName("partition").item(0);
        String clazz = partitionNode.getAttribute("class");
        logger.info("partition class : {}, ", clazz);
        try {
            Class<?> clz = Class.forName(clazz);
            AbstractPartition partition = (AbstractPartition) clz.newInstance();
            // 设置参数
            NodeList nodeList = partitionNode.getElementsByTagName("property");

            Map<String, String> kv = new HashMap<>();
            for (int i = 0; i < nodeList.getLength(); i++) {
                Element property = (Element) nodeList.item(i);
                String key = property.getAttribute("name");
                String value = property.getTextContent();
                kv.put(key, value);
            }

            partition.init(kv);
            PartitionConfig config = new PartitionConfig();
            config.setPartition(partition);
            config.setColumn(kv.get("partitionKey"));
//            Partition partition = Thread.currentThread().getContextClassLoader().loadClass(clazz);
//            Partition partition =  Class.forName(clazz);
            return config;
        } catch (ClassNotFoundException cnfe) {
            logger.error("partition class {} is not found", cnfe);
            throw new RuntimeException("partition class not found");
        } catch (InstantiationException e) {
            logger.error("partition class {} instantiate fail", e);
            throw new RuntimeException("partition class instantialize fail");
        } catch (IllegalAccessException e) {
            logger.error("partition class {} access fail", e);
            throw new RuntimeException("partition class access fail");
        }
    }

    public static List<TableConfig.NodeConfig> getNodes(Element root) {
//        NodeList Nodes = ((Element)tableNode.getElementsByTagName("datasource").item(0))
//                .getElementsByTagName("node");

        List<TableConfig.NodeConfig> nodes = new ArrayList<>();
        NodeList Nodes = ((Element) root.getElementsByTagName("datasource").item(0))
                .getElementsByTagName("node");
        for (int h = 0; h < Nodes.getLength(); h++) {
            Element nodeNode = (Element) Nodes.item(h);
//            tableConfig.getDatasource().add(
//                    new TableConfig.NodeConfig(
//                            nodeNode.getAttribute("databode"),
//                            nodeNode.getAttribute("database")));
            nodes.add(new TableConfig.NodeConfig(
                    nodeNode.getAttribute("datanode"),
                    nodeNode.getAttribute("database")));
        }
        return nodes;
    }

    public void loadSchema(Element root) {
        NodeList schemaNodes = root.getElementsByTagName("schema");
        for (int i = 0; i < schemaNodes.getLength(); i++) {
            Element schemaNode = (Element) schemaNodes.item(i);
            String schemaName = schemaNode.getAttribute("name");
//            getSchemaConfigs().put(schemaName, new SchemaConfig());
            getSchemaConfigs().put(schemaName.toUpperCase(), new SchemaConfig());
            NodeList tableNodes = schemaNode.getElementsByTagName("table");
            NodeList tablegroupNodes = schemaNode.getElementsByTagName("tablegroup");

//            schemaNode.getChildNodes()

            TableConfig tableConfig = new TableConfig();
            // instance.getSchemaConfig().tables;
            // deal with table
            for (int j = 0; j < tableNodes.getLength(); j++) {
                Element tableNode = (Element) tableNodes.item(j);
                String name = tableNode.getAttribute("name");

//                Element partitionNode = (Element)tableNode.getElementsByTagName("partition").item(0);
                if (!tableNode.getParentNode().getNodeName().equals("schema")) {
                    continue;
                }

                PartitionConfig config = getPartition(tableNode);
                tableConfig.setRule(config);
                tableConfig.setPartitionColumn(config.getColumn());

                tableConfig.setDatasource(getNodes(tableNode));
//                tableConfig.setName(name);
//                getSchemaConfigs().get(schemaName).getTables().put(name, tableConfig);
                tableConfig.setName(name.toUpperCase());
                getSchemaConfigs().get(schemaName.toUpperCase()).getTables().put(name.toUpperCase(), tableConfig);
            }

            for (int j = 0; j < tablegroupNodes.getLength(); j++) {
                Element tablegroupNode = (Element) tablegroupNodes.item(j);
                NodeList subTableNodes = ((Element) tablegroupNode.getElementsByTagName("tables").item(0))
                        .getElementsByTagName("table");
//                getPartition(tablegroupNode);
                PartitionConfig config = getPartition(tablegroupNode);
//                tableConfig.setRule(config);
//                tableConfig.setPartitionColumn(config.getColumn());

                List<TableConfig.NodeConfig> nodes = getNodes(tablegroupNode);
                for (int h = 0; h < subTableNodes.getLength(); h++) {
                    String name = ((Element) subTableNodes.item(h)).getAttribute("name");
                    // primary key is null
                    getSchemaConfigs().get(schemaName).getTables().put(name,
                            new TableConfig(name, config.getColumn(), nodes, null, config));
                }
            }
        }
    }


    public void loadDatasource(Element root) {
        NodeList datanodeNodes = root.getElementsByTagName("datanode");
        for (int i = 0; i < datanodeNodes.getLength(); i++) {
            Node node = datanodeNodes.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;

                DataSourceConfig.DatanodeConfig datanode = new DataSourceConfig.DatanodeConfig();
                DataSourceConfig.HostConfig writehost = new DataSourceConfig.HostConfig();
                List<DataSourceConfig.HostConfig> readhosts = new ArrayList<>();

                // name="d0" balance="rr" maxconn="100" minconn="10" readtype="1" dbtype="mysql" dbdriver="builtin"
                datanode.setName(e.getAttribute("name"));
                datanode.setBalance(e.getAttribute("balance"));
                datanode.setMaxconn(Integer.parseInt(e.getAttribute("maxconn")));
                datanode.setMinconn(Integer.parseInt(e.getAttribute("minconn")));
                datanode.setReadtype(Boolean.parseBoolean(e.getAttribute("readtype")));
                datanode.setDbtype(e.getAttribute("dbtype"));
                datanode.setDbdriver(e.getAttribute("dbdriver"));

//               <writehost url="localhost:3306" user="xujianhai" password="xujianhai"/>
//               <readhost url="localhost:3306" user="xujianhai" password="xujianhai"/>
//               <heartbeat>select user()</heartbeat>
                Element writehostNode = (Element) e.getElementsByTagName("writehost").item(0);
                writehost.setUrl(writehostNode.getAttribute("url"));
                writehost.setUser(writehostNode.getAttribute("user"));
                writehost.setPassword(writehostNode.getAttribute("password"));
                writehost.setReadType(false);
                datanode.setWritehost(writehost);

                NodeList readhostNodes = e.getElementsByTagName("readhost");
                for (int j = 0; j < readhostNodes.getLength(); j++) {
                    Element readhostNode = (Element) readhostNodes.item(j);
                    readhosts.add(new DataSourceConfig.HostConfig(
                            readhostNode.getAttribute("url"),
                            readhostNode.getAttribute("user"),
                            readhostNode.getAttribute("password"),
                            true,
                            Integer.parseInt(readhostNode.getAttribute("weight"))));
                }
//                logger.info("index : {}, datanode: {}", i, datanode);
                datanode.setReadhost(readhosts);
                getDatasource().getDatanodes().add(datanode);
            }
        }
    }

}
