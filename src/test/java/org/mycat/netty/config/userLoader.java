package org.mycat.netty.config;

import org.junit.Before;
import org.junit.Test;
import org.mycat.netty.mysql.auth.XmlPrivilege;
import org.mycat.netty.mysql.auth.YamlPrivilege;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;

/**
 * Created by snow_young on 16/8/6.
 */

public class userLoader {
    private static final Logger logger = LoggerFactory.getLogger(userLoader.class);

    @Before
    public void setUp()throws Exception {
        //  write to yaml, problem : write is not immediate
//        YamlPrivilege privilege = new YamlPrivilege();
//        privilege.getWhitehosts().put("127.0.0.1", "xujianhai,user,laoshan");
//        privilege.getWhitehosts().put("127.0.0.2", "xujianhai,user");
//
//        privilege.getUsers().put("xujianhai", new YamlPrivilege.UserConfig("xujianhai", "schema,testdb", true));
//        privilege.getUsers().put("user", new YamlPrivilege.UserConfig("xujianhai", "schema,testdb", true));
//        privilege.getUsers().put("laoshan", new YamlPrivilege.UserConfig("xujianhai", "schema,testdb", true));
//
//        // should be immutable
////         put the resurces address to property
//        File dumpFile = new File(System.getProperty("user.dir") + "/src/test/resources/user.yaml");
//        FileWriter fs = new FileWriter(dumpFile);
//        new Yaml().dump(privilege, fs);
//        fs.flush();
//
//        privilege.getUsers().clear();
//        privilege.getWhitehosts().clear();
    }

//    @Test
    public void testXMLLoad() throws Exception {
        new XmlPrivilege().load();
    }

    @Test
    public void testYAMLLoad() throws Exception {
        YamlPrivilege.load();
    }

    public void testTOMLLoad(){

    }
}