package com.bytehonor.sdk.starter.redis.service.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.util.CollectionUtils;

import java.util.Set;

import com.bytehonor.sdk.define.bytehonor.util.StringObject;
import com.bytehonor.sdk.lang.bytehonor.getter.IntegerGetter;
import com.bytehonor.sdk.lang.bytehonor.getter.LongGetter;
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
    public void kvSet(String key, String value) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(value)) {
            return;
        }

        redisLettuceDao.kvSet(key, value);
    }

    @Override
    public void kvSetAndExpire(String key, String value, long millis) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(value)) {
            return;
        }

        kvSet(key, value);
        expireAt(key, System.currentTimeMillis() + millis);
    }

    @Override
    public void delete(String key) {
        if (StringObject.isEmpty(key)) {
            return;
        }

        redisLettuceDao.delete(key);
    }

    @Override
    public void expireAt(String key, long timestamp) {
        if (StringObject.isEmpty(key)) {
            return;
        }

        redisLettuceDao.expireAt(key, timestamp);
    }

    @Override
    public void increment(String key) {
        if (StringObject.isEmpty(key)) {
            return;
        }

        redisLettuceDao.increment(key);
    }

    @Override
    public boolean lock(String key, long millis) {
        if (StringObject.isEmpty(key)) {
            return false;
        }

        return redisLettuceDao.kvSetIfAbsent(key, key, millis);
    }

    @Override
    public void resetCount(String key) {
        if (StringObject.isEmpty(key)) {
            return;
        }

        redisLettuceDao.kvSet(key, "0");
    }

    @Override
    public int getCount(String key) {
        if (StringObject.isEmpty(key)) {
            return 0;
        }

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
    public void hashIncrement(String key, String field) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(field)) {
            return;
        }

        redisLettuceDao.hashIncrement(key, field);
    }

    @Override
    public void hashDelete(String key, String field) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(field)) {
            return;
        }

        redisLettuceDao.hashDelete(key, field);
    }

    @Override
    public int hashSize(String key) {
        if (StringObject.isEmpty(key)) {
            return 0;
        }

        Long size = redisLettuceDao.hashSize(key);
        return size != null ? size.intValue() : 0;
    }

    @Override
    public int setSize(String key) {
        if (StringObject.isEmpty(key)) {
            return 0;
        }

        Long size = redisLettuceDao.setSize(key);
        return size != null ? size.intValue() : 0;
    }

    @Override
    public void setAdd(String key, String value) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(value)) {
            return;
        }

        redisLettuceDao.setAdd(key, value);
    }

    @Override
    public void setAdds(String key, Set<String> values) {
        if (StringObject.isEmpty(key) || CollectionUtils.isEmpty(values)) {
            return;
        }

        redisLettuceDao.setAdds(key, values);
    }

    @Override
    public boolean setContains(String key, String value) {
        if (StringObject.isEmpty(key) || StringObject.isEmpty(value)) {
            return false;
        }

        return redisLettuceDao.setContains(key, value);
    }

    @Override
    public Set<String> setMemebers(String key) {
        if (StringObject.isEmpty(key)) {
            return new HashSet<String>();
        }
        Set<Serializable> raws = redisLettuceDao.setMemebers(key);
        Set<String> result = new HashSet<String>(raws.size() * 2);
        for (Serializable raw : raws) {
            result.add(raw.toString());
        }
        return result;
    }

    @Override
    public void setLongAdd(String key, Long value) {
        if (StringObject.isEmpty(key) || value == null) {
            return;
        }

        redisLettuceDao.setAdd(key, value.toString());
    }

    @Override
    public void setLongAdds(String key, Set<Long> values) {
        if (StringObject.isEmpty(key) || CollectionUtils.isEmpty(values)) {
            return;
        }

        redisLettuceDao.setAdds(key, values.toArray());
    }
    
    public <T> Object[] toArray(Set<T> values) {
        Object[] arr = new Object[values.size()];
        int i = 0;
        for (T t : values) {
            arr[i++] = t;
        }
        return arr;
    }

    @Override
    public boolean setLongContains(String key, Long value) {
        if (StringObject.isEmpty(key) || value == null) {
            return false;
        }

        return redisLettuceDao.setContains(key, value);
    }

    @Override
    public Set<Long> setLongMemebers(String key) {
        if (StringObject.isEmpty(key)) {
            return new HashSet<Long>();
        }
        Set<Serializable> raws = redisLettuceDao.setMemebers(key);
        Set<Long> result = new HashSet<Long>(raws.size() * 2);
        for (Serializable raw : raws) {
            result.add(LongGetter.optional(raw.toString(), 0L));
        }
        return result;
    }

}
