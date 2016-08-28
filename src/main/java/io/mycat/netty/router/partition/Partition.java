package io.mycat.netty.router.partition;

import java.util.Map;

/**
 * Created by snow_young on 16/8/7.
 */
public interface Partition {
    void init(Map<String, String> params);
    int caculate(String columnValue);
    int[] calculateRange(String beginValue, String endValue);
}
