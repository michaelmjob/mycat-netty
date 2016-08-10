package io.mycat.netty.mysql.auth;

import java.util.Collection;
import java.util.List;

/**
 * Created by snow_young on 16/7/17.
 */
public interface Privilege {
    
    boolean userExists(String user);
    
    boolean schemaExists(String user, String schema);
    
    String password(String user);
    
    boolean checkPassword(String user, String password, String salt);

    Collection<String> getSchemas(String user);
}