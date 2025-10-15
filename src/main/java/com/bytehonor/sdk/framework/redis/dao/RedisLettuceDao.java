package com.bytehonor.sdk.framework.redis.dao;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import com.bytehonor.sdk.framework.lang.Java;

/**
 * @author lijianqiang
 *
 */
public class RedisLettuceDao {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLettuceDao.class);

    private final RedisTemplate<String, Serializable> redisTemplate;

    public RedisLettuceDao(RedisTemplate<String, Serializable> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Long hashIncrement(String key, String field) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(field, "field");

        return redisTemplate.opsForHash().increment(key, field, 1L);
    }

    public void hashPut(String key, String field, String val) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(field, "field");

        redisTemplate.opsForHash().put(key, field, val);
    }

    public Object hashGet(String key, String field) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(field, "field");

        return redisTemplate.opsForHash().get(key, field);
    }

    public void hashDelete(String key, Object... fields) {
        Java.requireNonNull(key, "key");

        redisTemplate.opsForHash().delete(key, fields);
    }

    public Long hashSize(String key) {
        Java.requireNonNull(key, "key");

        LOG.debug("hashSize {}", key);
        return redisTemplate.opsForHash().size(key);
    }

    public Set<Object> hashFields(String key) {
        Java.requireNonNull(key, "key");

        LOG.debug("hashFields {}", key);
        return redisTemplate.opsForHash().keys(key);
    }

    public Map<Object, Object> hashEntries(String key) {
        Java.requireNonNull(key, "key");

        LOG.debug("hashEntries {}", key);
        return redisTemplate.opsForHash().entries(key);
    }

    public void expire(String key, long timeout, TimeUnit unit) {
        Java.requireNonNull(key, "key");

        redisTemplate.expire(key, timeout, unit);
    }

    public void expireAt(String key, long timestamp) {
        Java.requireNonNull(key, "key");

        redisTemplate.expireAt(key, new Date(timestamp));
    }

    /* set 操作 */
    public Long setSize(String key) {
        Java.requireNonNull(key, "key");

        return redisTemplate.opsForSet().size(key);
    }

    public void setAdd(String key, String value) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(value, "value");

        redisTemplate.opsForSet().add(key, value);
    }

    /**
     * 注意，外层必须传 Serializable[], 如果传Object[], 整个Object[] 只会当成一个值
     * 
     * @param key
     * @param values
     */
    public void setAdds(String key, Serializable[] values) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(values, "values");

        redisTemplate.opsForSet().add(key, values);
    }

    public boolean setContains(String key, Object target) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(target, "target");

        return redisTemplate.opsForSet().isMember(key, target);
    }

    public Set<Serializable> setMemebers(String key) {
        Java.requireNonNull(key, "key");

        return redisTemplate.opsForSet().members(key);
    }

    public void setRemove(String key, String value) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(value, "value");

        redisTemplate.opsForSet().remove(key, value);
    }

    /* string 操作 */
    public void kvSet(String key, String value) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(value, "value");

        redisTemplate.opsForValue().set(key, value);
    }

    public void kvSetAndExpire(String key, String value, long timeoutMillis) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(value, "value");

        redisTemplate.opsForValue().set(key, value, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public String kvGet(String key) {
        Java.requireNonNull(key, "key");

        // (String) 不能强制转换
        Serializable val = redisTemplate.opsForValue().get(key);
        if (val == null) {
            return null;
        }
        return val.toString();
    }

    public boolean kvSetIfAbsent(String key, String value, long timeoutMillis) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(value, "value");

        return redisTemplate.opsForValue().setIfAbsent(key, value, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public boolean delete(String key) {
        Java.requireNonNull(key, "key");

        return redisTemplate.delete(key);
    }
    
    public boolean has(String key) {
        Java.requireNonNull(key, "key");
        
        return redisTemplate.hasKey(key);
    }

    public Long increment(String key) {
        Java.requireNonNull(key, "key");

        return redisTemplate.opsForValue().increment(key);
    }

    public Long decrement(String key) {
        Java.requireNonNull(key, "key");

        return redisTemplate.opsForValue().decrement(key);
    }

    public void putInteger(String key, Integer value) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(value, "value");

        redisTemplate.opsForValue().set(key, value);
    }

    public void putLong(String key, Long value) {
        Java.requireNonNull(key, "key");
        Java.requireNonNull(value, "value");

        redisTemplate.opsForValue().set(key, value);
    }

    
}
