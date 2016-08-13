package io.mycat.netty.util;

/**
 * Created by snow_young on 16/8/13.
 */
public class TimeUtil {
    private static volatile long CURRENT_TIME = System.currentTimeMillis();

    public static final long currentTimeMillis(){return CURRENT_TIME;}

    public static final long currentTimeNanos(){ return System.nanoTime();}

    public static final void update() { CURRENT_TIME = System.currentTimeMillis();}
}
