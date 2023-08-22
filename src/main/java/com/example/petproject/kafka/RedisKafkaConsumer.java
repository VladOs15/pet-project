package com.example.petproject.kafka;

import com.example.petproject.model.User;
import com.example.petproject.repository.UserDao;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

@Service
public class RedisKafkaConsumer {
    private final UserDao userDao;
    private final ObjectMapper objectMapper;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisKafkaConsumer.class);
    @Value("${spring.redis.host}")
    private String redisHost;
    @Value("${spring.redis.port}")
    private int redisPort;
    public RedisKafkaConsumer(UserDao userDao, ObjectMapper objectMapper) {
        this.userDao = userDao;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kafka_topic_redis", groupId = "myGroup")
    public void consumeUserMessage(User user){
        LOGGER.info("Получен JSON из Kafka: " + user);
        try {
            LOGGER.info(String.format("Сonsumer получил Json сообщение  -> %s", user));
            if (isRedisConnected()) {
                userDao.saveUser(objectMapper.writeValueAsString(user));
                LOGGER.info(String.format("User сохранил Json сообщение в Redis  -> %s", user));
            } else {
                LOGGER.error(String.format("Ошибка, данные не сохранены -> %s", user));
            }
        } catch (Exception e) {
            LOGGER.error("Ошибка при дессериализации сообщения из Redis: " + e.getMessage(), e);
        }
    }

    /**
     * Проверяет подключение к Redis.
     * @return true, если подключение успешно установлено, иначе false.
     */
    public boolean isRedisConnected() {
        try (Jedis jedis = new Jedis(redisHost, redisPort)){
            if (jedis.ping().equals("PONG")) {
                return true;
            } else {
                LOGGER.error(String.format("Ошибка подключения к Redis: Ответ не равен PONG, ответ: %s", jedis.ping()));
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Ошибка подключения к Redis: " + e.getMessage(), e);
            return false;
        }
    }

}
