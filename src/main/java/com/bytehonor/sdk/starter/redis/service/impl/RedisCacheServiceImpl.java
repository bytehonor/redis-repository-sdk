package com.bytehonor.sdk.starter.redis.service.impl;

import com.bytehonor.sdk.starter.redis.dao.RedisLettuceDao;
import com.bytehonor.sdk.starter.redis.service.RedisCacheService;

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
    public boolean lock(String key, String value, long millis) {
        return redisLettuceDao.kvSetIfAbsent(key, value, millis);
    }

}
