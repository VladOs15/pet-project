package com.example.petproject.kafka;

import com.example.petproject.model.User;
import com.example.petproject.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Service
public class KafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
    private final UserRepository userRepository;
    @Autowired
    private final DataSource dataSource;

    public KafkaConsumer(UserRepository userRepository, DataSource dataSource) {
        this.userRepository = userRepository;
        this.dataSource = dataSource;
    }

    @KafkaListener(topics = "kafka_topic_json", groupId = "myGroup")
    public void consume(User user){
        LOGGER.info(String.format("Сonsumer получил Json сообщение -> %s", user.toString()));
        if(isDatabaseConnected()) {
            userRepository.save(user);
            LOGGER.info(String.format("User сохранен в postgres usr -> %s", user));
        } else {
            LOGGER.error("Ошибка подключения к базе данных");
        }
    }

    /**
     * Проверяет подключение к Postgres.
     * @return true, если подключение успешно установлено, иначе false.
     */
    private boolean isDatabaseConnected() {
        try (Connection connection = (Connection) dataSource.getConnection()){
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}
