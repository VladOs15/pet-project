package com.example.petproject.repository;

import com.example.petproject.payload.User;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class UserDao {
    private static final String HASH_KEY = "User";

    private final RedisTemplate<String, Object> template;

    public UserDao(RedisTemplate<String, Object> template) {
        this.template = template;
    }

    public void saveUser(User user){
        template.opsForValue().set(HASH_KEY,user);
    }



}
