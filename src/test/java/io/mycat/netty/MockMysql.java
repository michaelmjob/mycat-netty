package io.mycat.netty;

import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.EmbeddedMysql;

import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.ScriptResolver.classPathScript;
import static com.wix.mysql.ScriptResolver.classPathScripts;
import static com.wix.mysql.distribution.Version.v5_6_23;
import static com.wix.mysql.config.Charset.UTF8;

/**
 * Created by snow_young on 16/9/8.
 */
public class MockMysql {

    public static void main(String[] args){
        MysqldConfig config = aMysqldConfig(v5_6_23)
                .withCharset(UTF8)
                .withPort(3306)
                .withUser("xujianhai", "xujianhai")
                .withTimeZone("Europe/Vilnius")
                .build();


        EmbeddedMysql mysqld = anEmbeddedMysql(config)
                .addSchema("db0", classPathScript("/db0.sql"))
                .addSchema("db1", classPathScripts("/db1.sql"))
                .start();

        //do work

        mysqld.stop(); //optional, as there is a shutdown hook
    }
}
