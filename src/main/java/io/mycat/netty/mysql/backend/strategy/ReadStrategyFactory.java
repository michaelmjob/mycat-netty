package io.mycat.netty.mysql.backend.strategy;

import io.mycat.netty.mysql.backend.datasource.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.Arrays;

/**
 * Created by snow_young on 16/9/12.
 */
public class ReadStrategyFactory {
    private static final Logger logger = LoggerFactory.getLogger(ReadStrategyFactory.class);

    public static ReadStrategy buildStrategy(String name, Host[] hosts){
        try {
            Class clz = Class.forName(name);

            Constructor constructor = clz.getConstructor();
            AbstractReadStrategy readStrategy = ((AbstractReadStrategy) constructor.newInstance());
            readStrategy.setHosts(hosts);
            return readStrategy;
        } catch (ClassNotFoundException e) {
            logger.error("read strategy for {} can not find", name, e);
        } catch (NoSuchMethodException e) {
            logger.error("no constructor for read strategy : {}d", name, e);
        } catch (InvocationTargetException|InstantiationException|IllegalAccessException e) {
            logger.error("initialize fail for read strategy : {}", name, e);
        }
        return new LeastConnStrategy(hosts);
    }
}
