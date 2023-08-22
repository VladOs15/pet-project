package com.example.petproject.kafka;

import com.example.petproject.model.User;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.Set;

@Service
public class RedisKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisKafkaProducer.class);
    private final KafkaTemplate<String, User> kafkaTemplate;
    public static final String TOPIC_REDIS = "kafka_topic_redis";
    private static final String ERROR_TOPIC = "error-topic";
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;

    public RedisKafkaProducer(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(User user){
        if (isKafkaReachable()) {
            if (isTopicAvailable()){
                kafkaTemplate.send(TOPIC_REDIS, user);
                LOGGER.info(String.format("Producer отправил Json сообщение -> %s", user.toString()));
            } else {
                handleKafkaError(user);
            }
        }else {
            LOGGER.error(String.format("Ошибка -> %s", user.toString()));
        }
    }

    /**
     * Проверяет подключение к Kafka.
     * @return true, если подключение успешно установлено, иначе false.
     */
    public boolean isKafkaReachable(){
        try{
            kafkaTemplate.send("test-topic", new User());
            return true;
        } catch (Exception e){
            LOGGER.error("Ошибка подключения к Kafka: " + e.getMessage(), e);
            return false;
        }
    }

    /**
     * Проверяет наличие топика в Kafka.
     * @return true, если топик успешно найден, иначе false.
     */
    public boolean isTopicAvailable(){
        try {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            try(AdminClient admin = AdminClient.create(properties)) {
                Set<String> topics = admin.listTopics().names().get();
                return topics.contains(TOPIC_REDIS);
            }
        }  catch (Exception e){
            LOGGER.error("Ошибка при проверке наличия топика: " + TOPIC_REDIS, e);
            return false;
        }
    }

    /**
     * Обрабатывает ошибку при подключении к топику Kafka.
     * Отправляет сообщение в дополнительный топик error-topic.
     *
     * @param user Пользователь, данные которого не удалось отправить в Kafka.
     */
    protected void handleKafkaError(User user){
        try {
            kafkaTemplate.send(ERROR_TOPIC, user);
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка при отправке в Kafka, сообщение отправлено в дополнительный топик");
        } catch (Exception e){
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка при отправке и в дополнительный топик");
        }
    }
}