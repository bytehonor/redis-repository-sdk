package com.bytehonor.sdk.starter.redis.service.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.util.CollectionUtils;

import com.bytehonor.sdk.lang.spring.getter.IntegerGetter;
import com.bytehonor.sdk.lang.spring.getter.LongGetter;
import com.bytehonor.sdk.lang.spring.string.SpringString;
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
        if (SpringString.isEmpty(key) || SpringString.isEmpty(value)) {
            return;
        }

        redisLettuceDao.kvSet(key, value);
    }

    @Override
    public void kvSetAndExpire(String key, String value, long millis) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(value)) {
            return;
        }

        redisLettuceDao.kvSetAndExpire(key, value, millis);
    }

    @Override
    public void delete(String key) {
        if (SpringString.isEmpty(key)) {
            return;
        }

        redisLettuceDao.delete(key);
    }

    @Override
    public boolean has(String key) {
        if (SpringString.isEmpty(key)) {
            return false;
        }
        return redisLettuceDao.has(key);
    }

    @Override
    public void expireAt(String key, long timestamp) {
        if (SpringString.isEmpty(key)) {
            return;
        }

        redisLettuceDao.expireAt(key, timestamp);
    }

    @Override
    public long increment(String key) {
        if (SpringString.isEmpty(key)) {
            return 0L;
        }

        Long val = redisLettuceDao.increment(key);
        return val != null ? val : 0L;
    }

    @Override
    public long decrement(String key) {
        if (SpringString.isEmpty(key)) {
            return 0L;
        }

        Long val = redisLettuceDao.decrement(key);
        return val != null ? val : 0L;
    }

    @Override
    public boolean lock(String key, long millis) {
        if (SpringString.isEmpty(key)) {
            return false;
        }

        return redisLettuceDao.kvSetIfAbsent(key, key, millis);
    }

    @Override
    public void putInteger(String key, Integer val) {
        if (SpringString.isEmpty(key)) {
            return;
        }

        redisLettuceDao.putInteger(key, val);
    }

    @Override
    public int getInteger(String key) {
        if (SpringString.isEmpty(key)) {
            return 0;
        }

        String val = redisLettuceDao.kvGet(key);
        return IntegerGetter.optional(val, 0);
    }

    @Override
    public long getLong(String key) {
        if (SpringString.isEmpty(key)) {
            return 0L;
        }

        String val = redisLettuceDao.kvGet(key);
        return LongGetter.optional(val, 0L);
    }

    @Override
    public void putLong(String key, Long val) {
        if (SpringString.isEmpty(key)) {
            return;
        }

        redisLettuceDao.putLong(key, val);
    }

    @Override
    public boolean kvSetIfAbsent(String key, String value, long millis) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(value)) {
            return false;
        }

        return redisLettuceDao.kvSetIfAbsent(key, value, millis);
    }

    @Override
    public Map<String, String> hashEntries(String key) {
        if (SpringString.isEmpty(key)) {
            return new HashMap<String, String>();
        }

        Map<Object, Object> raws = redisLettuceDao.hashEntries(key);
        if (raws == null || raws.isEmpty()) {
            return new HashMap<String, String>();
        }

        Map<String, String> result = new HashMap<String, String>(raws.size() * 2);
        for (Entry<Object, Object> item : raws.entrySet()) {
            result.put(String.valueOf(item.getKey()), String.valueOf(item.getKey()));
        }
        return result;
    }

    @Override
    public Map<String, Integer> hashIntEntries(String key) {
        if (SpringString.isEmpty(key)) {
            return new HashMap<String, Integer>();
        }

        Map<Object, Object> raws = redisLettuceDao.hashEntries(key);
        if (raws == null || raws.isEmpty()) {
            return new HashMap<String, Integer>();
        }

        Map<String, Integer> result = new HashMap<String, Integer>(raws.size() * 2);
        for (Entry<Object, Object> item : raws.entrySet()) {
            result.put(String.valueOf(item.getKey()), IntegerGetter.optional(String.valueOf(item.getKey())));
        }
        return result;
    }

    @Override
    public Integer hashIntGet(String key, String field) {
        String raw = hashGet(key, field);
        if (raw == null) {
            return null;
        }
        return IntegerGetter.optional(raw);
    }

    @Override
    public void hashIntPut(String key, String field, Integer val) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(field) || val == null) {
            return;
        }

        hashPut(key, field, val.toString());
    }

    @Override
    public Long hashLongGet(String key, String field) {
        String raw = hashGet(key, field);
        if (raw == null) {
            return null;
        }
        return LongGetter.optional(raw);
    }

    @Override
    public void hashLongPut(String key, String field, Long val) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(field) || val == null) {
            return;
        }

        hashPut(key, field, val.toString());
    }

    @Override
    public void hashIncrement(String key, String field) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(field)) {
            return;
        }

        redisLettuceDao.hashIncrement(key, field);
    }

    @Override
    public String hashGet(String key, String field) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(field)) {
            return null;
        }

        Object raw = redisLettuceDao.hashGet(key, field);
        if (raw == null) {
            return null;
        }
        return String.valueOf(raw);
    }

    @Override
    public void hashPut(String key, String field, String val) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(field) || val == null) {
            return;
        }

        redisLettuceDao.hashPut(key, field, val);
    }

    @Override
    public void hashDelete(String key, String field) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(field)) {
            return;
        }

        redisLettuceDao.hashDelete(key, field);
    }

    @Override
    public int hashSize(String key) {
        if (SpringString.isEmpty(key)) {
            return 0;
        }

        Long size = redisLettuceDao.hashSize(key);
        return size != null ? size.intValue() : 0;
    }

    @Override
    public int setSize(String key) {
        if (SpringString.isEmpty(key)) {
            return 0;
        }

        Long size = redisLettuceDao.setSize(key);
        return size != null ? size.intValue() : 0;
    }

    @Override
    public void setAdd(String key, String value) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(value)) {
            return;
        }

        redisLettuceDao.setAdd(key, value);
    }

    @Override
    public void setAdds(String key, Set<String> values) {
        if (SpringString.isEmpty(key) || CollectionUtils.isEmpty(values)) {
            return;
        }

        redisLettuceDao.setAdds(key, toArray(values));
    }

    private Serializable[] toArray(Set<String> values) {
        Serializable[] arr = new Serializable[values.size()];
        int i = 0;
        for (String value : values) {
            arr[i++] = value;
        }
        return arr;
    }

    @Override
    public boolean setContains(String key, String value) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(value)) {
            return false;
        }

        return redisLettuceDao.setContains(key, value);
    }

    @Override
    public Set<String> setMemebers(String key) {
        if (SpringString.isEmpty(key)) {
            return new HashSet<String>();
        }
        Set<Serializable> raws = redisLettuceDao.setMemebers(key);
        if (CollectionUtils.isEmpty(raws)) {
            return new HashSet<String>();
        }
        Set<String> result = new HashSet<String>(raws.size() * 2);
        for (Serializable raw : raws) {
            result.add(raw.toString());
        }
        return result;
    }

    @Override
    public void setLongAdd(String key, Long value) {
        if (SpringString.isEmpty(key) || value == null) {
            return;
        }

        redisLettuceDao.setAdd(key, value.toString());
    }

    @Override
    public void setLongAdds(String key, Set<Long> values) {
        if (SpringString.isEmpty(key) || CollectionUtils.isEmpty(values)) {
            return;
        }

        redisLettuceDao.setAdds(key, toLongArray(values));
    }

    private Serializable[] toLongArray(Set<Long> values) {
        Serializable[] arr = new Serializable[values.size()];
        int i = 0;
        for (Long value : values) {
            arr[i++] = value;
        }
        return arr;
    }

    @Override
    public boolean setLongContains(String key, Long value) {
        if (SpringString.isEmpty(key) || value == null) {
            return false;
        }

        return redisLettuceDao.setContains(key, value);
    }

    @Override
    public Set<Long> setLongMemebers(String key) {
        if (SpringString.isEmpty(key)) {
            return new HashSet<Long>();
        }
        Set<Serializable> raws = redisLettuceDao.setMemebers(key);
        if (CollectionUtils.isEmpty(raws)) {
            return new HashSet<Long>();
        }
        Set<Long> result = new HashSet<Long>(raws.size() * 2);
        for (Serializable raw : raws) {
            result.add(LongGetter.optional(raw.toString(), 0L));
        }
        return result;
    }

    @Override
    public void setRemove(String key, String value) {
        if (SpringString.isEmpty(key) || SpringString.isEmpty(value)) {
            return;
        }

        redisLettuceDao.setRemove(key, value);
    }

}
