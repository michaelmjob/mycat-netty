package io.mycat.netty.mysql.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by snow_young on 16/8/7.
 */
public class PrivilegeFactory {
    private static final Logger logger = LoggerFactory.getLogger(PrivilegeFactory.class);

    // todo: refactor privilege, prop privilege is diffrent
    private static volatile Privilege privilege;
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

        public boolean checkPassword(String user, byte[] password, String salt) {
            return true;
        }

        public List<String> getSchemas(String user) { return null; }
    };

    public static void loadPrivilege(String file){
        // seek config file
        XmlPrivilege xmlPrivilege = new XmlPrivilege();
        try {
            xmlPrivilege.load();
        } catch (Exception e) {
            logger.info("load file[{}] with xml occurs error", file, e);
        }

        privilege = xmlPrivilege;
    }

    public static Privilege getPrivilege(){
        return privilege;
    }


}
