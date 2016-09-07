package io.mycat.netty;

import org.junit.Test;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Created by snow_young on 16/9/7.
 */
public class H2Test {
    private static final Logger logger = LoggerFactory.getLogger(H2Test.class);

    @Test
    public void testSQL() throws SQLException {
        String[] args = new String[4];
        args[0] = "-tcpPort";
        args[1] = "3306";
        args[2] = "-tcpPassword";
        args[3] = "xujianhai";
        Server server = Server.createTcpServer(args).start();



        logger.info("stop!");
        server.stop();
    }
}
