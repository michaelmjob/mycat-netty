package io.mycat.netty.conf;

/**
 * Created by snow_young on 16/8/13.
 */
public class SystemConfig {

    // fixed value
    public static int packetHeaderSize = 4;
    public static int maxPacketSize = 16 * 1024 * 1024;

    public static final int DEFAULT_BUFFER_SIZE = 16384;
    // usename pass schema , if root xujianhai
    public static final int HANDSHAKE_BUFFER_SIZE = 100;
}
