package com.example.petproject.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;
@Repository
public class UserDao {
    private static final String KEY_PREFIX = "User:";
    private final StringRedisTemplate template;
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDao.class);

    public UserDao(StringRedisTemplate template) {
        this.template = template;
    }

    public void saveUser(String userData) {
        try {
            template.opsForValue().set(KEY_PREFIX, userData);
        } catch (Exception e) {
            LOGGER.error("Ошибка при сохранении данных в Redis: " + e.getMessage(), e);
        }
    }

    public String getUserFromRedis() {
        try {
            return template.opsForValue().get(KEY_PREFIX);
        } catch (Exception e) {
            LOGGER.error("Ошибка при получении данных из Redis: " + e.getMessage(), e);
        }
        return null;
    }
}

