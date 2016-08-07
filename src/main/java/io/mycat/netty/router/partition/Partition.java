package io.mycat.netty.router.partition;

/**
 * Created by snow_young on 16/8/7.
 */
public interface Partition {
    void init();
    int caculate(String columnValue);
    int[] calculateRange(String beginValue, String endValue);
}
