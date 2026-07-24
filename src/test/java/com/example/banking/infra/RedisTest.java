package com.example.banking.infra;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class RedisTest {

    @Autowired
    StringRedisTemplate redisTemplate;

    @Test
    void redisRoundTrip() {
        redisTemplate.opsForValue().set("skeleton:key", "value");
        assertThat(redisTemplate.opsForValue().get("skeleton:key")).isEqualTo("value");
    }
}
