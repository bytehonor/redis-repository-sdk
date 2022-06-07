package com.bytehonor.sdk.starter.redis.service.impl;

import com.bytehonor.sdk.lang.bytehonor.getter.IntegerGetter;
import com.bytehonor.sdk.starter.redis.dao.RedisLettuceDao;
import com.bytehonor.sdk.starter.redis.service.RedisCacheService;
import com.bytehonor.sdk.starter.redis.util.RedisSdkUtils;

public class RedisCacheServiceImpl implements RedisCacheService {

    private final RedisLettuceDao redisLettuceDao;

    public RedisCacheServiceImpl(RedisLettuceDao redisLettuceDao) {
        this.redisLettuceDao = redisLettuceDao;
    }

    @Override
    public String kvGet(String key) {
        return redisLettuceDao.kvGet(key);
    }

    @Override
    public void kvPut(String key, String value) {
        redisLettuceDao.kvSet(key, value);
    }

    @Override
    public void delete(String key) {
        redisLettuceDao.keyDel(key);
    }

    @Override
    public void expireAt(String key, long timestamp) {
        redisLettuceDao.expireAt(key, timestamp);
    }

    @Override
    public void increament(String key) {
        redisLettuceDao.keyIncreament(key);
    }

    @Override
    public boolean lock(String key, long millis) {
        if (RedisSdkUtils.isEmpty(key)) {
            return false;
        }
        return redisLettuceDao.kvSetIfAbsent(key, key, millis);
    }

    @Override
    public void resetCount(String key) {
        redisLettuceDao.kvSet(key, "0");
    }

    @Override
    public int getCount(String key) {
        String val = redisLettuceDao.kvGet(key);
        return IntegerGetter.optional(val, 0);
    }

}
