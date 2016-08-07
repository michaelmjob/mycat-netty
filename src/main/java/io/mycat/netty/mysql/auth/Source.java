package io.mycat.netty.mysql.auth;

/**
 * Created by snow_young on 16/8/7.
 * load resurce from file(xml yaml toml or db or zk)
 */
public interface Source {

    void load() throws Exception;
}
