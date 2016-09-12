package io.mycat.netty.mysql.auth;

import com.google.common.base.Strings;
import io.mycat.netty.util.SecurityUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by snow_young on 16/8/7.
 */
@Data
@AllArgsConstructor
public abstract class AbstractPrivilege implements Privilege{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPrivilege.class);

    // host -> userlist with ','
    protected Map<String, String> whitehosts;
    // name -> password, schemas
    protected Map<String, UserConfig> users ;

    public AbstractPrivilege(){
        whitehosts = new HashMap<>();
        users = new HashMap<>();
    }


    @Override
    public boolean userExists(String user) {
        return users.containsKey(user);
    }

    @Override
    public boolean schemaExists(String user, String schema) {
        return users.get(user).getSchemas().contains(schema);
    }

    @Override
    public String password(String user) {
        return users.get(user).getPassword();
    }

    // TODO: move encryption algorithm, here is natvie
    @Override
    public boolean checkPassword(String user, byte[] password, String salt) {
        logger.info("check password for user {} with pass {} and salt {}", user, password, salt);
        String localPass = password(user);
        try {
            if(Strings.isNullOrEmpty(localPass)){
                return true;
            }
            byte[] encryptPass411 = SecurityUtil.scramble411(localPass, salt);
            logger.info("411 : {}", encryptPass411);
            if(Arrays.equals(encryptPass411, password)){
                return true;
            }
        } catch (Exception e) {
            logger.info("validate User:{} and Password:{} with salt:{} occur error, 411 pass : {}", user, password, salt,e);
        }
        return false;
    }

    public Collection<String> getSchemas(String user){
        return users.get(user).getSortedSchemas();
    }


    // TODO: change schemas structure implementation with set
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserConfig {
        private String password;
        private String schemas;
//        private Set<String> schemas;
        private boolean readOnly;

        private Set<String> sortedSchemas;

        public UserConfig(String password, String schemas, boolean readOnly){
            this.password = password;
            this.schemas = schemas;
            this.readOnly = readOnly;

            this.sortedSchemas = new TreeSet(Arrays.asList(this.schemas.split(",")));
            logger.info("treeset schemas : {}", this.sortedSchemas);
        }

    }
}
