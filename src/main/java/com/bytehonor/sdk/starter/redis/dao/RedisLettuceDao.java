package com.bytehonor.sdk.starter.redis.dao;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import com.bytehonor.sdk.starter.redis.util.RedisSdkUtils;

public class RedisLettuceDao {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLettuceDao.class);

    private final RedisTemplate<String, Serializable> redisTemplate;

    public RedisLettuceDao(RedisTemplate<String, Serializable> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Long hashIncrement(String key, String hashKey) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(hashKey, "hashKey");
        key = RedisSdkUtils.format(key);
        return redisTemplate.opsForHash().increment(key, hashKey, 1L);
    }

    public void hashPut(String key, String hashKey, String val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(hashKey, "hashKey");
        key = RedisSdkUtils.format(key);
        redisTemplate.opsForHash().put(key, hashKey, val);
    }

    public Object hashGet(String key, String hashKey) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(hashKey, "hashKey");
        key = RedisSdkUtils.format(key);
        return redisTemplate.opsForHash().get(key, hashKey);
    }

    public void hashDelete(String key, Object... hashKeys) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        redisTemplate.opsForHash().delete(key, hashKeys);
    }

    /**
     * 同一个key下不同的field
     * 
     * @param key
     * @param increaments
     */
    public void hashIncreamentBatch(String key, final Map<String, Integer> increaments) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(increaments, "increaments");
        if (increaments.isEmpty()) {
            return;
        }
        key = RedisSdkUtils.format(key);
        final byte[] kc = key.getBytes();
        redisTemplate.execute(new RedisCallback<Long>() {

            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();
                for (Map.Entry<String, Integer> item : increaments.entrySet()) {
                    if (RedisSdkUtils.isEmpty(item.getKey()) || item.getValue() == null) {
                        continue;
                    }
                    connection.hIncrBy(kc, item.getKey().getBytes(), item.getValue());
                }
                return null;
            }

        });
    }

    /**
     * 不同的key和field
     * 
     * @param hashMap
     */
    public void hashIncreamentMulti(final Map<String, String> hashMap) {
        Objects.requireNonNull(hashMap, "hashMap");
        if (hashMap.isEmpty()) {
            return;
        }
        redisTemplate.execute(new RedisCallback<Integer>() {

            @Override
            public Integer doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();
                for (String k : hashMap.keySet()) {
                    String key = RedisSdkUtils.format(k);
                    String feild = hashMap.get(k);
                    if (RedisSdkUtils.isEmpty(feild)) {
                        continue;
                    }
                    connection.hIncrBy(key.getBytes(), feild.getBytes(), 1L);
                }
                return null;
            }

        });
    }

    public Long hashSize(String key) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        LOG.debug("hashSize {}", key);
        return redisTemplate.opsForHash().size(key);
    }

    public Set<Object> hashKeys(String key) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        LOG.debug("hashKeys {}", key);
        return redisTemplate.opsForHash().keys(key);
    }

    public Map<Object, Object> hashEntries(String key) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        LOG.debug("hashEntries {}", key);
        return redisTemplate.opsForHash().entries(key);
    }

    public void expire(String key, long timeout, TimeUnit unit) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        redisTemplate.expire(key, timeout, unit);
    }

    public void expireBatchSeconds(final Map<String, Long> map) {
        Objects.requireNonNull(map, "map");
        if (map.isEmpty()) {
            return;
        }
        redisTemplate.execute(new RedisCallback<Long>() {

            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();
                for (String rawkey : map.keySet()) {
                    String key = RedisSdkUtils.format(rawkey);
                    connection.expire(key.getBytes(), map.get(rawkey));
                }
                return null;
            }

        });
    }

    public void expireAt(String key, long timestamp) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        redisTemplate.expireAt(key, new Date(timestamp));
    }

    public void expireAtBatchSeconds(final Map<String, Long> map) {
        redisTemplate.execute(new RedisCallback<Long>() {

            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();
                for (String rawkey : map.keySet()) {
                    String key = RedisSdkUtils.format(rawkey);
                    connection.expireAt(key.getBytes(), map.get(rawkey));
                }
                return null;
            }

        });
    }

    /* set 操作 */

    public void setAdd(String key, Serializable... values) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(values, "values");
        key = RedisSdkUtils.format(key);
        redisTemplate.opsForSet().add(key, values);
    }

    public boolean setIsMemeberOrNot(String key, Object target) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(target, "target");
        key = RedisSdkUtils.format(key);
        return redisTemplate.opsForSet().isMember(key, target);
    }

    public Set<Serializable> setMemebers(String key) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        return redisTemplate.opsForSet().members(key);
    }

    public void setDel(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        key = RedisSdkUtils.format(key);
        redisTemplate.opsForSet().remove(key, value);
    }

    /* string 操作 */

    public void kvSet(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        key = RedisSdkUtils.format(key);
        redisTemplate.opsForValue().set(key, value);
    }

    public String kvGet(String key) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        return (String) redisTemplate.opsForValue().get(key);
    }

    public boolean kvSetIfAbsent(String key, String value, long timeout) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        key = RedisSdkUtils.format(key);
        return redisTemplate.opsForValue().setIfAbsent(key, value, timeout, TimeUnit.MILLISECONDS);
    }

    public boolean keyDel(String key) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        return redisTemplate.delete(key);
    }

    public void keyIncreament(String key) {
        Objects.requireNonNull(key, "key");
        key = RedisSdkUtils.format(key);
        redisTemplate.opsForValue().increment(key);
    }
    
}
