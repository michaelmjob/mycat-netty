package org.mycat.netty.mysql.auth;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by snow_young on 16/8/6.
 */
@Data
public class YamlPrivilege {
    private static final Logger logger = LoggerFactory.getLogger(YamlPrivilege.class);
    private static YamlPrivilege instance;

    // host -> userlist with ','
//    private static  Map<String, String> whitehosts ;
    private Map<String, String> whitehosts ;
//            = new HashMap<>();
    // name -> password, schemas
//    private static Map<String, UserConfig> users ;
    private Map<String, UserConfig> users ;
//    = new HashMap<>();


    public YamlPrivilege(){
        users = new HashMap<>();
        whitehosts = new HashMap<>();
    }

    public static void load() throws Exception {
        // save to yaml
        Yaml yaml = new Yaml();
        InputStream is = XmlPrivilege.class.getResourceAsStream("/user.yaml");
        instance = yaml.loadAs(is, YamlPrivilege.class);
        logger.info("yaml privilege : {}", instance);

//        write
//        whitehosts.put("127.0.0.1", "xujianhai,user,laoshan");
//        whitehosts.put("127.0.0.2", "xujianhai,user");
//
//        users.put("xujianhai", new UserConfig("xujianhai", "schema,testdb", true));
//        users.put("user", new UserConfig("xujianhai", "schema,testdb", true));
//        users.put("laoshan", new UserConfig("xujianhai", "schema,testdb", true));
//
//        File dumpFile = new File(System.getProperty("user.dir") + "/src/test/resources/user.yaml");
//        yaml.dump(this, new FileWriter(dumpFile));




//        Thread.currentThread().getContextClassLoader().getResource("");
//        File file = XmlPrivilege.class.getClassLoader().getResource("/user.yaml").getFile();
//        yaml.dump(whitehosts, new FileWriter(new OutputStreamWriter()));
//        yaml.dump(users);
//        logger.info("users : " + map.get("users"));
//        logger.info("user.yaml : ", map);
    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private class UserConfig{
        private String password;
        private String schemas;
        private boolean readOnly;
    }
}
