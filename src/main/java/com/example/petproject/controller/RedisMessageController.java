package com.example.petproject.controller;

import com.example.petproject.Exception.InvalidJsonFormatException;
import com.example.petproject.Exception.KafkaConnectionException;
import com.example.petproject.Exception.RedisConnectException;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import javax.validation.Valid;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


@Validated
@RestController
@RequestMapping("/api/v1/kafka_post")
public class RedisMessageController {
    private final RedisKafkaProducer kafkaProducer;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMessageController.class);
    private static final String TOPIC = "kafka_topic_json";
    private static final String ERROR_TOPIC = "error-topic";
    @Value("${spring.redis.host}")
    private String redisHost;
    @Value("${spring.redis.port}")
    private int redisPort;

    public RedisMessageController(RedisKafkaProducer KafkaProducer) {
        this.kafkaProducer = KafkaProducer;
        this.objectMapper = new ObjectMapper();
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
    public ResponseEntity<String> publishToRedis(@RequestBody @Valid User user){
        try {
            if(!isKafkaConnected()){
                handleKafkaError(user);
                throw new KafkaConnectionException("Отсутсвует подключение к Kafka.");
            }

            if(!isRedisConnected()){
                throw new RedisConnectException("Отсутсвует подключение к Redis.");
            }

            try {
                objectMapper.readTree(objectMapper.writeValueAsString(user));
                if (user.getName() == null || user.getName().isEmpty()){
                    return ResponseEntity.badRequest().body("Имя пользователя не указано");
                }
                if (user.getAge() <= 17){
                    return ResponseEntity.badRequest().body("Неверный возраст пользователя");
                }
                if (user.getWork() == null || user.getWork().isEmpty()){
                    return ResponseEntity.badRequest().body("Должность пользователя не указана");
                }
                kafkaProducer.sendMessage(user);
                return ResponseEntity.ok(String.format("Сообщение Json отправлено в topic: %s", user));
            } catch (JsonProcessingException e){
                throw new InvalidJsonFormatException("Ошибка: невалидный JSON формат " + e.getOriginalMessage().substring((int) e.getLocation().getCharOffset()), e);
            }
        } catch (KafkaConnectionException e){
            LOGGER.error("Ошибка при отправке сообщения в Kafka: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Ошибка при отправке сообщения: " + e.getMessage());
        } catch (RedisConnectException e){
            LOGGER.error("Ошибка при отправке сообщения в Redis: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Ошибка при отправке сообщения: " + e.getMessage());
        } catch (InvalidJsonFormatException e){
            LOGGER.error("Ошибка при отправке сообщения в JSON формате: ", e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Ошибка при отправке сообщения в JSON формате: " + e.getMessage());
        } catch (Exception e){
            LOGGER.error("Ошибка при отправке сообщения: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка: " + e.getMessage());
        }
    }

    /**
     * Проверяет подключение к Redis.
     * @return true, если подключение успешно установлено, иначе false.
     */
    private boolean isRedisConnected() {
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
            AdminClient adminClient = AdminClient.create(properties);
            if (isTopicExists(TOPIC, adminClient)) {
                return true;
            } else {
                LOGGER.error("Ошибка подключения к Kafka: Топик " + TOPIC + " не существует");
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Ошибка подключения к Kafka: " + e.getMessage(), e);
            return false;
        }
    }

    private boolean isTopicExists(String topic, AdminClient adminClient) {
        try {
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topicNames = topicsResult.names().get();
            return topicNames.contains(topic);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Ошибка при проверке наличия топика: " + topic, e);
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
