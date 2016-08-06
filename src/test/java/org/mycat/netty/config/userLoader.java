package org.mycat.netty.config;

import org.junit.Test;
import org.mycat.netty.mysql.auth.XmlPrivilege;
import org.mycat.netty.mysql.auth.YamlPrivilege;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/8/6.
 */

public class userLoader {
    private static final Logger logger = LoggerFactory.getLogger(userLoader.class);



//    @Test
    public void testXMLLoad() throws Exception {
        new XmlPrivilege().load();
    }

    @Test
    public void testYAMLLoad() throws Exception {

//        write
        YamlPrivilege privilege = new YamlPrivilege();
        whitehosts.put("127.0.0.1", "xujianhai,user,laoshan");
        whitehosts.put("127.0.0.2", "xujianhai,user");

        users.put("xujianhai", new UserConfig("xujianhai", "schema,testdb", true));
        users.put("user", new UserConfig("xujianhai", "schema,testdb", true));
        users.put("laoshan", new UserConfig("xujianhai", "schema,testdb", true));

        File dumpFile = new File(System.getProperty("user.dir") + "/src/test/resources/user.yaml");
        yaml.dump(this, new FileWriter(dumpFile));

        Thread.currentThread().getContextClassLoader().getResource("");
        File file = XmlPrivilege.class.getClassLoader().getResource("/user.yaml").getFile();
        yaml.dump(whitehosts, new FileWriter(new OutputStreamWriter()));
        yaml.dump(users);
        logger.info("users : " + map.get("users"));
        logger.info("user.yaml : ", map);



        YamlPrivilege.load();



    }

    public void testTOMLLoad(){

    }
}