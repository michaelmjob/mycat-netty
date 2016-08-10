package io.mycat.netty.mysql.auth;

import io.mycat.netty.util.StringUtil;
import io.mycat.netty.util.SecurityUtil;
import io.mycat.netty.util.SysProperties;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by snow_young on 16/7/17.
 */
public class PropPrivilege implements Privilege, Source {
    
    private static final Logger logger = LoggerFactory.getLogger(PropPrivilege.class);

    private Properties prop;

    public PropPrivilege(){};

    public PropPrivilege(Properties users) {
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

    @Override
    public List<String> getSchemas(String user) {
        return Arrays.asList(prop.getProperty(user+".password").split(","));
    }

    @Override
    public void load() throws Exception {
        InputStream source = PropPrivilege.class.getResourceAsStream("user.properties");
        if(source == null) {
            logger.error("Can't load privilege config from user.properties");
            System.exit(-1);
        }
        try {
            Properties prop = new Properties();
            prop.load(source);
        } catch (Exception e) {
            logger.info("error load privilege config from user.properties", e);
            System.exit(-1);
        }
    }
}
