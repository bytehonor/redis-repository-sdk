package com.bytehonor.sdk.starter.redis.service;

import java.util.Map;

public interface RedisCacheService {

    public void delete(String key);

    public boolean lock(String key, long millis);

    public void expireAt(String key, long timestamp);

    public String kvGet(String key);

    public void kvSet(String key, String value);
    
    public void kvSetAndTtl(String key, String value, long millis);

    public boolean kvSetIfAbsent(String key, String value, long millis);

    public void increament(String key);

    public void resetCount(String key);

    public int getCount(String key);
    
    public Map<String, Integer> hashIntEntries(String key);

    public Integer hashIntGet(String key, String field);
    
    public void hashIntPut(String key, String field, Integer val);
    
    public void hashDelete(String key, String field);
    
    public int hashSize(String key);
}
