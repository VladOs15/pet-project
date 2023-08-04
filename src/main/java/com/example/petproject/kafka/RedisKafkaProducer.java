package com.example.petproject.kafka;

import com.example.petproject.payload.User;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class RedisKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisKafkaProducer.class);
    private KafkaTemplate<String, User> kafkaTemplate;
    public String topic = "kafka_topic_redis";

    public RedisKafkaProducer(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(User user){
//        throw new KafkaException("Ошибка при отправке сообщения в Kafka");
        LOGGER.info(String.format("Producer отправил Json сообщение -> %s", user.toString()));

        Message<User> message = MessageBuilder
                .withPayload(user)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();

        kafkaTemplate.send(message);
    }
}
