package com.example.petproject.kafka;

import com.example.petproject.payload.User;
import com.example.petproject.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
    private UserRepository userRepository;

    public KafkaConsumer(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @KafkaListener(topics = "kafka_topic_json", groupId = "myGroup")
    public void consume(User user){
        LOGGER.info(String.format("Сonsumer получил Json сообщение  -> %s", user.toString()));
        userRepository.save(user);
        LOGGER.info(String.format("User сохранил Json сообщение в db usr  -> %s", user));
    }

}
