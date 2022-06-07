package com.bytehonor.sdk.starter.redis.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.bytehonor.sdk.define.bytehonor.util.StringObject;
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
    public void kvSet(String key, String value) {
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

    @Override
    public boolean kvSetIfAbsent(String key, String value, long millis) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(value)) {
            return false;
        }
        return redisLettuceDao.kvSetIfAbsent(key, value, millis);
    }

    @Override
    public Map<String, Integer> hashIntEntries(String key) {
        if (StringObject.isEmpty(key)) {
            return new HashMap<String, Integer>();
        }
        Map<Object, Object> raws = redisLettuceDao.hashEntries(key);
        Map<String, Integer> result = new HashMap<String, Integer>(raws.size() * 2);
        for (Entry<Object, Object> item : raws.entrySet()) {
            result.put(String.valueOf(item.getKey()), IntegerGetter.optional(String.valueOf(item.getKey())));
        }
        return result;
    }

    @Override
    public Integer hashIntGet(String key, String field) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(field)) {
            return null;
        }
        Object raw = redisLettuceDao.hashGet(key, field);
        if (raw == null) {
            return null;
        }
        return IntegerGetter.optional(String.valueOf(raw));
    }

    @Override
    public void hashIntPut(String key, String field, Integer val) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(field) || val == null) {
            return;
        }
        redisLettuceDao.hashPut(key, field, val.toString());
    }

    @Override
    public void hashDelete(String key, String field) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(field)) {
            return;
        }
        redisLettuceDao.hashDelete(key, field);
    }

    @Override
    public long hashSize(String key) {
        if (StringObject.isEmpty(key)) {
            return 0L;
        }
        Long size = redisLettuceDao.hashSize(key);
        return size != null ? size : 0L;
    }

}
