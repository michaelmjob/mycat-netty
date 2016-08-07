package io.mycat.netty.mysql.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by snow_young on 16/8/7.
 */
public class PrivilegeFactory {
    private static final Logger logger = LoggerFactory.getLogger(PrivilegeFactory.class);

    public static final Privilege TRUE_PRIVILEGE = new Privilege(){

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

    public static Privilege getPrivilege(String file){
        // seek config file
        XmlPrivilege xmlPrivilege = new XmlPrivilege();
        try {
            xmlPrivilege.load();
        } catch (Exception e) {
            logger.info("load file[{}] with xml occurs error", file, e);
        }
        return xmlPrivilege;
    }
}
