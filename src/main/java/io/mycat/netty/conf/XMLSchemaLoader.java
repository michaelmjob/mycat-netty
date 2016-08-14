package io.mycat.netty.conf;

import io.mycat.netty.mysql.auth.XmlUtils;
import io.mycat.netty.router.partition.Partition;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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

    private Map<String, SchemaConfig> schemaConfigs;
    private DataSourceConfig datasource;

    public XMLSchemaLoader(){
        schemaConfigs =  new HashMap<>();
        datasource = new DataSourceConfig();
    }

    public void load() throws IOException, SAXException, ParserConfigurationException {
        InputStream dtd = XMLSchemaLoader.class.getResourceAsStream("/schema.dtd");
        InputStream xml = XMLSchemaLoader.class.getResourceAsStream("/schema.xml");
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


    public static Partition getPartition(Element root){
        Element partitionNode = (Element)root.getElementsByTagName("partition").item(0);
        String clazz = partitionNode.getAttribute("class");
        logger.info("partition class : {}, ", clazz);

        return null;
    }

    public static List<TableConfig.Node> getNodes(Element root){
//        NodeList Nodes = ((Element)tableNode.getElementsByTagName("datasource").item(0))
//                .getElementsByTagName("node");

        List<TableConfig.Node> nodes = new ArrayList<>();
        NodeList Nodes = ((Element)root.getElementsByTagName("datasource").item(0))
                .getElementsByTagName("node");
        for(int h = 0 ; h < Nodes.getLength(); h++){
            Element nodeNode = (Element) Nodes.item(h);
//            tableConfig.getDatasource().add(
//                    new TableConfig.Node(
//                            nodeNode.getAttribute("databode"),
//                            nodeNode.getAttribute("database")));
            nodes.add(new TableConfig.Node(
                    nodeNode.getAttribute("datanode"),
                    nodeNode.getAttribute("database")));
        }
        return nodes;
    }

    public void loadSchema(Element root){
        NodeList schemaNodes = root.getElementsByTagName("schema");
        for(int i = 0; i < schemaNodes.getLength(); i++){
            Element schemaNode = (Element)schemaNodes.item(i);
            String schemaName = schemaNode.getAttribute("name");
            getSchemaConfigs().put(schemaName, new SchemaConfig());
            NodeList tableNodes = schemaNode.getElementsByTagName("table");
            NodeList tablegroupNodes = schemaNode.getElementsByTagName("tablegroup");

//            schemaNode.getChildNodes()

            TableConfig tableConfig =  new TableConfig();
            // instance.getSchemaConfig().tables;
            // deal with table
            for(int j = 0; j < tableNodes.getLength(); j++){
                Element tableNode = (Element)tableNodes.item(j);
                String name = tableNode.getAttribute("name");

//                Element partitionNode = (Element)tableNode.getElementsByTagName("partition").item(0);
                if(!tableNode.getParentNode().getNodeName().equals("schema")){
                    continue;
                }

                getPartition(tableNode);

                tableConfig.setDatasource(getNodes(tableNode));

                tableConfig.setName(name);
                getSchemaConfigs().get(schemaName).getTables().put(name, tableConfig);
            }

            for(int j = 0 ; j < tablegroupNodes.getLength(); j++){
                Element tablegroupNode = (Element) tablegroupNodes.item(j);
                NodeList subTableNodes = ((Element)tablegroupNode.getElementsByTagName("tables").item(0))
                                                                        .getElementsByTagName("table");
                getPartition(tablegroupNode);
                List<TableConfig.Node> nodes = getNodes(tablegroupNode);
                for(int h = 0; h < subTableNodes.getLength(); h++){
                    String name = ((Element)subTableNodes.item(h)).getAttribute("name");
                    getSchemaConfigs().get(schemaName).getTables().put(name, new TableConfig(name, null, nodes));
                }
            }
        }
    }


    public void loadDatasource(Element root){
        NodeList datanodeNodes = root.getElementsByTagName("datanode");
        for(int i = 0; i < datanodeNodes.getLength(); i++){
            Node node = datanodeNodes.item(i);
            if(node instanceof Element){
                Element e = (Element)node;

                DataSourceConfig.DatanodeConfig datanode = new DataSourceConfig.DatanodeConfig();
                DataSourceConfig.HostConfig writehost= new DataSourceConfig.HostConfig();
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
                Element writehostNode = (Element)e.getElementsByTagName("writehost").item(0);
                writehost.setUrl(writehostNode.getAttribute("url"));
                writehost.setUser(writehostNode.getAttribute("user"));
                writehost.setPassword(writehostNode.getAttribute("password"));
                writehost.setReadType(false);
                datanode.setWritehost(writehost);

                NodeList readhostNodes = e.getElementsByTagName("readhost");
                for(int j = 0; j < readhostNodes.getLength(); j++){
                    Element readhostNode = (Element) readhostNodes.item(j);
                    readhosts.add(new DataSourceConfig.HostConfig(readhostNode.getAttribute("url"),
                                                        readhostNode.getAttribute("user"),
                                                        readhostNode.getAttribute("password"), true));
                }
//                logger.info("index : {}, datanode: {}", i, datanode);
                datanode.setReadhost(readhosts);
                getDatasource().getDatanodes().add(datanode);
            }
        }
    }

}
