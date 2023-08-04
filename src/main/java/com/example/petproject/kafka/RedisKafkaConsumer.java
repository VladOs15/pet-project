package com.example.petproject.kafka;

import com.example.petproject.payload.User;
import com.example.petproject.repository.UserDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RedisKafkaConsumer {
    private final UserDao userDao;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisKafkaConsumer.class);

    public RedisKafkaConsumer(UserDao userDao) {
        this.userDao = userDao;
    }

    @KafkaListener(topics = "kafka_topic_redis", groupId = "myGroup")
    public void consumeUserMessage(User user){
        LOGGER.info(String.format("Сonsumer получил Json сообщение  -> %s", user.toString()));
        userDao.saveUser(user);
        LOGGER.info(String.format("User сохранил Json сообщение в Redis  -> %s", user));
    }

}
