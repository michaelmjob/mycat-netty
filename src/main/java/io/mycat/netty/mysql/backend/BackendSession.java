package io.mycat.netty.mysql.backend;

import java.io.Closeable;

/**
 * Created by snow_young on 16/8/12.
 */
public interface BackendSession extends Closeable{
    String getCurrentDB();
}
