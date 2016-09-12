package io.mycat.netty.mysql.backend.strategy;

import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlHost;
import io.mycat.netty.util.Constants;
import junit.framework.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by snow_young on 16/9/12.
 */
public class ReadStrategyFactoryTest {

    @Test
    public void test(){

        Host host = Mockito.spy(Host.class);
        Host [] hosts = new Host[]{host};
        ReadStrategy readStrategy = ReadStrategyFactory.buildStrategy(Constants.READ_STRATEGY, hosts);
        Assert.assertTrue(readStrategy instanceof LeastConnStrategy);
    }
}
