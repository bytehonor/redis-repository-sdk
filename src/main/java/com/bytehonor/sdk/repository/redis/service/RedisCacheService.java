package com.bytehonor.sdk.repository.redis.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.util.CollectionUtils;

import com.bytehonor.sdk.framework.lang.getter.IntegerGetter;
import com.bytehonor.sdk.framework.lang.getter.LongGetter;
import com.bytehonor.sdk.framework.lang.string.StringKit;
import com.bytehonor.sdk.repository.redis.dao.RedisLettuceDao;

public class RedisCacheService {

    private final RedisLettuceDao redisLettuceDao;

    public RedisCacheService(RedisLettuceDao redisLettuceDao) {
        this.redisLettuceDao = redisLettuceDao;
    }

    public String kvGet(String key) {
        return redisLettuceDao.kvGet(key);
    }

    public void kvSet(String key, String value) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(value)) {
            return;
        }

        redisLettuceDao.kvSet(key, value);
    }

    public void kvSetAndExpire(String key, String value, long millis) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(value)) {
            return;
        }

        redisLettuceDao.kvSetAndExpire(key, value, millis);
    }

    public void delete(String key) {
        if (StringKit.isEmpty(key)) {
            return;
        }

        redisLettuceDao.delete(key);
    }

    public boolean has(String key) {
        if (StringKit.isEmpty(key)) {
            return false;
        }
        return redisLettuceDao.has(key);
    }

    public void expireAt(String key, long timestamp) {
        if (StringKit.isEmpty(key)) {
            return;
        }

        redisLettuceDao.expireAt(key, timestamp);
    }

    public long increment(String key) {
        if (StringKit.isEmpty(key)) {
            return 0L;
        }

        Long val = redisLettuceDao.increment(key);
        return val != null ? val : 0L;
    }

    public long decrement(String key) {
        if (StringKit.isEmpty(key)) {
            return 0L;
        }

        Long val = redisLettuceDao.decrement(key);
        return val != null ? val : 0L;
    }

    public boolean lock(String key, long millis) {
        if (StringKit.isEmpty(key)) {
            return false;
        }

        return redisLettuceDao.kvSetIfAbsent(key, key, millis);
    }

    public void putInteger(String key, Integer val) {
        if (StringKit.isEmpty(key)) {
            return;
        }

        redisLettuceDao.putInteger(key, val);
    }

    public int getInteger(String key) {
        if (StringKit.isEmpty(key)) {
            return 0;
        }

        String val = redisLettuceDao.kvGet(key);
        return IntegerGetter.optional(val, 0);
    }

    public long getLong(String key) {
        if (StringKit.isEmpty(key)) {
            return 0L;
        }

        String val = redisLettuceDao.kvGet(key);
        return LongGetter.optional(val, 0L);
    }

    public void putLong(String key, Long val) {
        if (StringKit.isEmpty(key)) {
            return;
        }

        redisLettuceDao.putLong(key, val);
    }

    public boolean kvSetIfAbsent(String key, String value, long millis) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(value)) {
            return false;
        }

        return redisLettuceDao.kvSetIfAbsent(key, value, millis);
    }

    public Map<String, String> hashEntries(String key) {
        if (StringKit.isEmpty(key)) {
            return new HashMap<String, String>();
        }

        Map<Object, Object> raws = redisLettuceDao.hashEntries(key);
        if (raws == null || raws.isEmpty()) {
            return new HashMap<String, String>();
        }

        Map<String, String> result = new HashMap<String, String>(raws.size() * 2);
        for (Entry<Object, Object> item : raws.entrySet()) {
            result.put(String.valueOf(item.getKey()), String.valueOf(item.getValue()));
        }
        return result;
    }

    public Map<String, Integer> hashIntEntries(String key) {
        if (StringKit.isEmpty(key)) {
            return new HashMap<String, Integer>();
        }

        Map<Object, Object> raws = redisLettuceDao.hashEntries(key);
        if (raws == null || raws.isEmpty()) {
            return new HashMap<String, Integer>();
        }

        Map<String, Integer> result = new HashMap<String, Integer>(raws.size() * 2);
        for (Entry<Object, Object> item : raws.entrySet()) {
            result.put(String.valueOf(item.getKey()), IntegerGetter.optional(String.valueOf(item.getValue())));
        }
        return result;
    }

    public Integer hashIntGet(String key, String field) {
        String raw = hashGet(key, field);
        if (raw == null) {
            return null;
        }
        return IntegerGetter.optional(raw);
    }

    public void hashIntPut(String key, String field, Integer val) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(field) || val == null) {
            return;
        }

        hashPut(key, field, val.toString());
    }

    public Long hashLongGet(String key, String field) {
        String raw = hashGet(key, field);
        if (raw == null) {
            return null;
        }
        return LongGetter.optional(raw);
    }

    public void hashLongPut(String key, String field, Long val) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(field) || val == null) {
            return;
        }

        hashPut(key, field, val.toString());
    }

    public void hashIncrement(String key, String field) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(field)) {
            return;
        }

        redisLettuceDao.hashIncrement(key, field);
    }

    public String hashGet(String key, String field) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(field)) {
            return null;
        }

        Object raw = redisLettuceDao.hashGet(key, field);
        if (raw == null) {
            return null;
        }
        return String.valueOf(raw);
    }

    public void hashPut(String key, String field, String val) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(field) || val == null) {
            return;
        }

        redisLettuceDao.hashPut(key, field, val);
    }

    public void hashDelete(String key, String field) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(field)) {
            return;
        }

        redisLettuceDao.hashDelete(key, field);
    }

    public int hashSize(String key) {
        if (StringKit.isEmpty(key)) {
            return 0;
        }

        Long size = redisLettuceDao.hashSize(key);
        return size != null ? size.intValue() : 0;
    }

    public int setSize(String key) {
        if (StringKit.isEmpty(key)) {
            return 0;
        }

        Long size = redisLettuceDao.setSize(key);
        return size != null ? size.intValue() : 0;
    }

    public void setAdd(String key, String value) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(value)) {
            return;
        }

        redisLettuceDao.setAdd(key, value);
    }

    public void setAdds(String key, Set<String> values) {
        if (StringKit.isEmpty(key) || CollectionUtils.isEmpty(values)) {
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

    public boolean setContains(String key, String value) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(value)) {
            return false;
        }

        return redisLettuceDao.setContains(key, value);
    }

    public Set<String> setMemebers(String key) {
        if (StringKit.isEmpty(key)) {
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

    public void setLongAdd(String key, Long value) {
        if (StringKit.isEmpty(key) || value == null) {
            return;
        }

        redisLettuceDao.setAdd(key, value.toString());
    }

    public void setLongAdds(String key, Set<Long> values) {
        if (StringKit.isEmpty(key) || CollectionUtils.isEmpty(values)) {
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

    public boolean setLongContains(String key, Long value) {
        if (StringKit.isEmpty(key) || value == null) {
            return false;
        }

        return redisLettuceDao.setContains(key, value);
    }

    public Set<Long> setLongMemebers(String key) {
        if (StringKit.isEmpty(key)) {
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

    public void setRemove(String key, String value) {
        if (StringKit.isEmpty(key) || StringKit.isEmpty(value)) {
            return;
        }

        redisLettuceDao.setRemove(key, value);
    }

}
