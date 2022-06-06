package com.bytehonor.sdk.starter.redis.util;

import java.util.Objects;

public class RedisSdkUtils {

    private static final String PREFIX = "bytehonor:";

    @Deprecated
    public static String format(String key) {
        Objects.requireNonNull(key, "key");

        if (key.startsWith(PREFIX) == false) {
            return new StringBuilder(PREFIX).append(key).toString();
        }
        return key;
    }

    public static boolean isEmpty(String src) {
        return (src == null || src.isEmpty());
    }
}
