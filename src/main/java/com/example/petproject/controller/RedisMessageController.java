package com.example.petproject.controller;

import com.example.petproject.kafka.RedisKafkaProducer;
import com.example.petproject.payload.User;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka_post")
public class RedisMessageController {
    private RedisKafkaProducer KafkaProducer;

    public RedisMessageController(RedisKafkaProducer KafkaProducer) {
        this.KafkaProducer = KafkaProducer;
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
        KafkaProducer.sendMessage(user);
        return ResponseEntity.ok("Сообщение Json отправленно в topic: " + user);
    }
}
