package com.bytehonor.sdk.framework.redis.util;

public class RedisKeyUtils {

    private static final String SPL = ":";

    public static String key(String... keys) {
        if (keys == null || keys.length < 1) {
            throw new RuntimeException("keys cannt be empty");
        }
        StringBuilder sb = new StringBuilder();
        int length = keys.length;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                sb.append(SPL);
            }
            sb.append(keys[i]);
        }
        return sb.toString();
    }

    public static String any(Object... keys) {
        if (keys == null || keys.length < 1) {
            throw new RuntimeException("keys cannt be empty");
        }
        StringBuilder sb = new StringBuilder();
        int length = keys.length;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                sb.append(SPL);
            }
            sb.append(keys[i]);
        }
        return sb.toString();
    }
}
