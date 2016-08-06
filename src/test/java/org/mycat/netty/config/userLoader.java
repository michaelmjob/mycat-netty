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
        YamlPrivilege.load();
    }

    public void testTOMLLoad(){

    }
}