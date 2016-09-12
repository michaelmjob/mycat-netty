package io.mycat.netty.mysql.backend.strategy;

import io.mycat.netty.mysql.backend.datasource.ConMap;
import io.mycat.netty.mysql.backend.datasource.Host;
import io.mycat.netty.mysql.backend.datasource.MysqlHost;
import junit.framework.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Created by snow_young on 16/9/12.
 */
public class LeastConnStrategyTest {


    @Test
    public void test(){

        Host host1 = Mockito.spy(Host.class);
        host1.setName("host1");
//        Mockito.stub(host1.connectionSize("dbname1", true)).toReturn(10);
//        Mockito.stub(host1.connectionSize("dbname2", true)).toReturn(8);

        Mockito.when(host1.connectionSize("dbname1", true)).thenReturn(10,  5, 1);

        // 10 5 0
//        Mockito.when(host1.connectionSize("dbname1", true)).thenAnswer(new Answer() {
//            int count1 = 15;
//
//            @Override
//            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
//                Object[] args = invocationOnMock.getArguments();
//                Object mock = invocationOnMock.getMock();
//                System.out.println("here");
//                count1 -= 5;
//                return count1;
//            }
//        });



        Host host2 = Mockito.spy(Host.class);
        host2.setName("host2");
        Mockito.when(host2.connectionSize("dbname1", true)).thenReturn(1, 5, 10);

//        Mockito.stub(host2.connectionSize("dbname1", true)).toReturn(9);
//        Mockito.stub(host2.connectionSize("dbname2", true)).toReturn(9);

        // 8  16
//        Mockito.when(host2.connectionSize("dbname1", true)).thenAnswer(new Answer() {
//            int count2 = 2;
//            @Override
//            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
//                Object[] args = invocationOnMock.getArguments();
//                Object mock = invocationOnMock.getMock();
//                System.out.println("here");
//                count2 += 6;
//                return count2 ;
//            }
//        });


        Host host3 = Mockito.spy(Host.class);
        host3.setName("host3");
        Mockito.when(host3.connectionSize("dbname1", true)).thenReturn(1, 10, 5);
//        Mockito.stub(host3.connectionSize("dbname1", true)).toReturn(8);
//        Mockito.stub(host3.connectionSize("dbname2", true)).toReturn(10);

//        Mockito.when(host3.connectionSize("dbname1", true)).thenAnswer(new Answer() {
//            int count3 = 5;
//
//            @Override
//            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
//                Object[] args = invocationOnMock.getArguments();
//                Object mock = invocationOnMock.getMock();
//                System.out.println("here");
//                count3 += 4;
//                return count3;
//            }
//        });

        Host[] hosts = new Host[]{host1, host2, host3};

        ReadStrategy readStrategy = new LeastConnStrategy(hosts);

        Assert.assertEquals(host1.getName(), readStrategy.select("dbname1").getName());
        Assert.assertEquals(host3.getName(), readStrategy.select("dbname1").getName());
        Assert.assertEquals(host2.getName(), readStrategy.select("dbname1").getName());

    }
}
