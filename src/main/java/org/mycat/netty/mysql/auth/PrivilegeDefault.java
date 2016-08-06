package org.mycat.netty.mysql.auth;

import org.mycat.netty.util.SecurityUtil;
import org.mycat.netty.util.StringUtil;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by snow_young on 16/7/17.
 */
public class PrivilegeDefault implements Privilege {
    
    private static final Logger logger = LoggerFactory.getLogger(PrivilegeDefault.class);
        
    private static final Privilege TRUE_PRIVILEGE = new Privilege(){

        public boolean userExists(String user) {
            return true;
        }

        public boolean schemaExists(String user, String schema) {
            return true;
        }

        public String password(String user) {
            return null;
        }

        public boolean checkPassword(String user, String password, String salt) {
            return true;
        }
    };


    private final Properties prop;

    private PrivilegeDefault(Properties users) {
        this.prop = users;
    }

    public boolean userExists(String user) {
        logger.info("check userExists {}", user);
        String property = prop.getProperty("users");
        String[] users = StringUtil.split(property, ',', true);
        return Arrays.asList(users).contains(user);
    }

    public boolean schemaExists(String user, String schema) {
        logger.info("check schemaexists {} for user {}", schema, user);
        String property = prop.getProperty(user + ".schemas");
        String[] schemas = StringUtil.split(property, ',', true);
        return Arrays.asList(schemas).contains(schema);
    }

    public String password(String user) {
        return prop.getProperty(user + ".password");
    }

    public boolean checkPassword(String user, String password, String salt) {
        logger.info("check password for user {} with pass {} and salt {}", user, password, salt);
        String localPass = password(user);
        try {
            if(StringUtil.isEmpty(localPass) && StringUtil.isEmpty(password)){
                return true;
            }
            String encryptPass411 = Base64.encodeBase64String(SecurityUtil.scramble411(localPass, salt));
            if(encryptPass411.equals(password)){
                return true;
            }
        } catch (Exception e) {
            logger.info("validateUserPassword error", e);
        }
        return false;
    }

    //TODO: refactor here
    public static Privilege getPrivilege() {
//        String path = SysProperties.SERVERUSER_CONFIG_LOCATION;
        String path = "./";
//        InputStream source = Utils.getResourceAsStream(path);
        InputStream source = null;
        if(source == null) {
            logger.info("Can't load privilege config from {}, using ", path);
            return TRUE_PRIVILEGE;
        }
        try {
            logger.info("using privilege config from {}" , path);
            Properties prop = new Properties();
            prop.load(source);
            return new PrivilegeDefault(prop);
        } catch (Exception e) {
            logger.info("error load privilege config from " + path, e);
            return TRUE_PRIVILEGE;
        }
    
    }
}
