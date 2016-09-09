package io.mycat.netty;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by snow_young on 16/9/9.
 */
public class TestBefore2 extends TestBefore1{

    @BeforeClass
    public static void beforeClass2(){
        System.out.println("beforeClass 2");
    }

    @Test
    public void test2(){
        System.out.println("test 2");
    }
}
