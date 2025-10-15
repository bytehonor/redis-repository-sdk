package com.bytehonor.sdk.framework.redis.config;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.bytehonor.sdk.framework.redis.dao.RedisLettuceDao;
import com.bytehonor.sdk.framework.redis.service.RedisCacheService;
import com.bytehonor.sdk.framework.redis.service.impl.RedisCacheServiceImpl;

/**
 * @author lijianqiang
 *
 */
@Configuration
@ConditionalOnClass({ RedisOperations.class, RedisProperties.class })
@AutoConfigureBefore(RedisAutoConfiguration.class)
public class RedisFrameworkConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(RedisFrameworkConfiguration.class);

    @Bean
    RedisTemplate<String, Serializable> redisTemplate(LettuceConnectionFactory connectionFactory) {
        LOG.info("[Bytehonor] RedisTemplate");
        RedisTemplate<String, Serializable> template = new RedisTemplate<String, Serializable>();
        StringRedisSerializer serializer = new StringRedisSerializer();
        template.setKeySerializer(serializer);
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashKeySerializer(serializer);
        template.setHashValueSerializer(serializer);
        template.setConnectionFactory(connectionFactory);
        return template;
    }

    @ConditionalOnMissingBean(RedisLettuceDao.class)
    @Bean
    RedisLettuceDao redisLettuceDao(RedisTemplate<String, Serializable> redisTemplate) {
        LOG.info("[Bytehonor] RedisLettuceDao");
        return new RedisLettuceDao(redisTemplate);
    }

    @ConditionalOnMissingBean(RedisCacheService.class)
    @Bean
    RedisCacheService redisCacheService(RedisLettuceDao redisLettuceDao) {
        LOG.info("[Bytehonor] RedisCacheService");
        return new RedisCacheServiceImpl(redisLettuceDao);
    }
}
