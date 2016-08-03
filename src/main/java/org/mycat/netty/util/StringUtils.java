package org.mycat.netty.util;

/**
 * Created by snow_young on 16/8/3.
 */
public class StringUtils {
    // TODO: use guava
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }
}
