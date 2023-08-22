package com.example.petproject.controller;

import com.example.petproject.kafka.KafkaProducer;
import com.example.petproject.model.User;
import com.example.petproject.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/kafka_postgres")
public class MessageController {
    private final KafkaProducer kafkaProducer;
    private final ObjectMapper objectMapper;
    private final UserRepository userRepo;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMessageController.class);

    public MessageController(KafkaProducer kafkaProducer, ObjectMapper objectMapper, UserRepository userRepo) {
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
        this.userRepo = userRepo;
    }

    /*
    http://localhost:8081//api/v1/kafka_postgres/publish
    {
        "id": 1,
            "name": "name",
            "age": 44,
            "work": "work"
    }*/
    @PostMapping("/publish")
    public ResponseEntity<String> publishToPostgres(@RequestBody User user){
        try {
            objectMapper.readTree(objectMapper.writeValueAsString(user));
            if (user.getName() == null
                    || user.getName().isEmpty()
                    || user.getAge() <= 0
                    || user.getWork() == null
                    || user.getWork().isEmpty()) {
                return ResponseEntity.badRequest().body("Данные введены не верно");
            }
            kafkaProducer.sendMessage(user);
            return ResponseEntity.ok(String.format("Сообщение Json отправлено в topic: %s", user));
        } catch (Exception e){
            LOGGER.error("Ошибка при отправке сообщения: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка: " + e.getMessage());
        }
    }

    //http://localhost:8081//api/v1/kafka_postgres/user_list
    @GetMapping("/user_list")
    public List<User> userList(){
        return userRepo.findAll();
    }

    //http://localhost:8081//api/v1/kafka_postgres/remove/
    @DeleteMapping("/remove/{id}")
    public ResponseEntity<String> remove(@PathVariable Long id){
        Optional<User> userOptional = userRepo.findById(id);

        if(userOptional.isPresent()){
            userRepo.deleteById(id);
            return ResponseEntity.ok("Пользователь с ID: " + id + " был удален");
        } else {
            return ResponseEntity.ok("Пользователь с ID: " + id + " не найден");
        }
    }
}
