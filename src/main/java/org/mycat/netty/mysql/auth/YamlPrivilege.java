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
//        instance = this;
    }

    public static void load() throws Exception {
        // save to yaml
        Yaml yaml = new Yaml();
        InputStream is = XmlPrivilege.class.getResourceAsStream("/user.yaml");
        instance = yaml.loadAs(is, YamlPrivilege.class);
        logger.info("yaml privilege : {}", instance);
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
