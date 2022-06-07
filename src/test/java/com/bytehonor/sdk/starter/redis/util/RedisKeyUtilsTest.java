package com.bytehonor.sdk.starter.redis.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisKeyUtilsTest {

    private static final Logger LOG = LoggerFactory.getLogger(RedisKeyUtilsTest.class);

    @Test
    public void test() {
        String key1 = RedisKeyUtils.any("a", "b", "c", 1, 2, 3L, "d");
        LOG.info("{}", key1);
    }

}
