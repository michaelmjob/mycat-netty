package io.mycat.netty.mysql.backend.strategy;

import io.mycat.netty.mysql.backend.datasource.Host;

/**
 * Created by snow_young on 16/9/12.
 */
public interface ReadStrategy {
    Host select(String database);
}
