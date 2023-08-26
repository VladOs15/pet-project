package com.example.petproject.controller;

import com.example.petproject.kafka.RedisKafkaProducer;
import com.example.petproject.model.User;
import com.example.petproject.repository.UserDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.ValidationException;

@Validated
@RestController
@RequestMapping("/api/v1/kafka_redis")
public class RedisMessageController {
    private final RedisKafkaProducer kafkaProducer;
    private final ObjectMapper objectMapper;
    private final UserDao userDao;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMessageController.class);

    public RedisMessageController(RedisKafkaProducer KafkaProducer, UserDao userDao) {
        this.kafkaProducer = KafkaProducer;
        this.userDao = userDao;
        this.objectMapper = new ObjectMapper();
    }

    /*
    http://localhost:8081//api/v1/kafka_redis/publish
    {
        "id": 1,
        "name": "name",
        "age": 44,
        "work": "work"
    }
    */
    @PostMapping("/publish")
    public ResponseEntity<String> publishToRedis(@RequestBody @Valid User user){
        try {
            objectMapper.readTree(objectMapper.writeValueAsString(user));
            validateUserData(user);
            kafkaProducer.sendMessage(user);
            return ResponseEntity.ok(String.format("Сообщение Json отправлено в topic: %s", user));
        } catch (JsonProcessingException e){
            LOGGER.error("Ошибка при преобразовании объекта: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Ошибка при преобразовании объекта: " + e.getMessage());
        } catch (ValidationException e){
            LOGGER.error("Ошибка валидации данных: ", e);
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e){
            LOGGER.error("Ошибка при отправке сообщения: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка: " + e.getMessage());
        }
    }

    //http://localhost:8081//api/v1/kafka_redis/user
    @GetMapping("/user")
    public ResponseEntity<String> getUserFromRedis() {
        String user = userDao.getUserFromRedis();
        return user != null ? ResponseEntity.ok(user) : ResponseEntity.notFound().build();
    }

    private void validateUserData(User user) throws ValidationException{
        if (user.getName() == null
                || user.getName().isEmpty()
                || user.getAge() <= 0
                || user.getWork() == null
                || user.getWork().isEmpty()) {
            throw new ValidationException("Данные введены не верно");
        }
    }
}

