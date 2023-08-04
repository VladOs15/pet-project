package com.example.petproject.controller;

import com.example.petproject.kafka.KafkaProducer;
import com.example.petproject.payload.User;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/kafka_post")
public class MessageController {
    private KafkaProducer kafkaProducer;

    public MessageController(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    /*
    http://localhost:8081//api/v1/kafka_post/publish
    {
        "id": 1,
            "name": "name",
            "age": 44,
            "work": "work"
    }*/
    @PostMapping("/publish")
    public ResponseEntity<String> publishToPostgres(@RequestBody User user){
        kafkaProducer.sendMessage(user);
        return ResponseEntity.ok("Сообщение Json отправленно в topic: " + user);
    }
}
