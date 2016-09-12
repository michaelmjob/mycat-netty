package io.mycat.netty.mysql.backend.strategy;

import io.mycat.netty.mysql.backend.datasource.Host;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by snow_young on 16/9/12.
 */
@NoArgsConstructor
public class LeastConnStrategy extends AbstractReadStrategy implements ReadStrategy{
    private static final Logger logger = LoggerFactory.getLogger(LeastConnStrategy.class);

    public LeastConnStrategy(Host[] hosts) {
        super(hosts);
    }

    @Override
    public Host select(String database) {
        int max = 0;
        Host target = null;
        for(Host host : hosts){
            int hostSize = host.connectionSize(database, true);
            if(hostSize > max){
                max = hostSize;
                target = host;
            }
        }
        return target;
    }
}
