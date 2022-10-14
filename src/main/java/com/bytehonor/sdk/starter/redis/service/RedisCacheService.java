package com.bytehonor.sdk.starter.redis.service;

import java.util.Map;
import java.util.Set;

public interface RedisCacheService {

    public void delete(String key);

    public boolean lock(String key, long millis);

    public void expireAt(String key, long timestamp);

    public String kvGet(String key);

    public void kvSet(String key, String value);

    public void kvSetAndExpire(String key, String value, long millis);

    public boolean kvSetIfAbsent(String key, String value, long millis);

    public long increment(String key);

    public long decrement(String key);

    public void putInteger(String key, Integer val);

    public int getInteger(String key);

    public long getLong(String key);

    public Map<String, Integer> hashIntEntries(String key);

    public Integer hashIntGet(String key, String field);

    public void hashIntPut(String key, String field, Integer val);

    public Long hashLongGet(String key, String field);

    public void hashLongPut(String key, String field, Long val);

    public void hashIncrement(String key, String field);

    public void hashDelete(String key, String field);

    public String hashGet(String key, String field);

    public void hashPut(String key, String field, String val);

    public int hashSize(String key);

    public int setSize(String key);

    public void setAdd(String key, String value);

    public void setAdds(String key, Set<String> values);

    public boolean setContains(String key, String value);

    public Set<String> setMemebers(String key);

    public void setLongAdd(String key, Long value);

    public void setLongAdds(String key, Set<Long> values);

    public boolean setLongContains(String key, Long value);

    public Set<Long> setLongMemebers(String key);
}
