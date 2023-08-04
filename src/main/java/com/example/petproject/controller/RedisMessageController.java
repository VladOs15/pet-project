package com.example.petproject.controller;

import com.example.petproject.kafka.RedisKafkaProducer;
import com.example.petproject.payload.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


@RestController
@RequestMapping("/api/v1/kafka_post")
public class RedisMessageController {
    private final RedisKafkaProducer kafkaProducer;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMessageController.class);
    private static final String TOPIC = "kafka_topic_json";

    private static final String ERROR_TOPIC = "error-topic";

    public RedisMessageController(RedisKafkaProducer KafkaProducer) {
        this.kafkaProducer = KafkaProducer;
    }

    /*
    http://localhost:8081//api/v1/kafka_post/publish/redis
    {
        "id": 1,
        "name": "name",
        "age": 44,
        "work": "work"
    }
    */
    @PostMapping("/publish/redis")
    public ResponseEntity<String> publishToRedis(@RequestBody User user){
        try {
            if(!isKafkaConnected()){
                handleKafkaError(user);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Ошибка: Отсутствует подключение к Kafka.");
            }

            if(!isRedisConnected()){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Ошибка: Отсутствует подключение к Redis.");
            }

            try {
                objectMapper.readTree(objectMapper.writeValueAsString(user));

                if (user.getName() == null || user.getName().isEmpty()){
                    return ResponseEntity.badRequest().body("Имя пользователя не указано");
                }
                if (user.getAge() <= 0){
                    return ResponseEntity.badRequest().body("Неверный возраст пользователя");
                }
                if (user.getWork() == null || user.getWork().isEmpty()){
                    return ResponseEntity.badRequest().body("Должность пользователя не указана");
                }
                kafkaProducer.sendMessage(user);
                return ResponseEntity.ok(String.format("Сообщение Json отправленно в topic: %s", user));
            } catch (JsonProcessingException e){
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Ошибка: невалидный JSON формат");
            }
        } catch (Exception e){
            LOGGER.error("Ошибка при отправке сообщения: ", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка: " + e.getMessage());
        }
    }

    /**
     * Проверяет подключение к Redis.
     * @return true, если подключение успешно установлено, иначе false.
     */
    private boolean isRedisConnected() {
        try (Jedis jedis = new Jedis("localhost", 6379)){
            if (jedis.ping().equals("PONG")) {
                return true;
            } else {
                LOGGER.error("Ошибка подключения к Redis: Ответ не равен PONG");
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Ошибка подключения к Redis: " + e.getMessage(), e);
            return false;
        }
    }

    /**
     * Проверяет подключение к Kafka.
     * @return true, если подключение успешно установлено, иначе false.
     */
    private boolean isKafkaConnected() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)){
            return isTopicExists(TOPIC, properties);
        } catch (Exception e) {
            LOGGER.error("Ошибка подключения к Kafka: " + e.getMessage(), e);
            return false;
        }
    }

    private boolean isTopicExists(String topic, Properties properties) {
        try {
            AdminClient adminClient = AdminClient.create(properties);
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topicNames = topicsResult.names().get();
            return topicNames.contains(topic);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Топик " + TOPIC + " не существует", e);
            return false;
        }
    }

    /**
     * Обрабатывает ошибку при подключении к Kafka.
     * Отправляет сообщение в дополнительный топик error-topic.
     * @param user Пользователь, данные которого не удалось отправить в Kafka.
     */
    private ResponseEntity<String> handleKafkaError(User user){
        try {
            kafkaTemplate.send(ERROR_TOPIC, objectMapper.writeValueAsString(user));
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка при отправке в Kafka, сообщение отправлено в дополнительный топик");
        } catch (Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка при отправке и в дополнительный топик");
        }
    }

}
