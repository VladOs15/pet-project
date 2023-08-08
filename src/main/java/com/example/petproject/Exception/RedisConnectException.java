package com.example.petproject.Exception;

public class RedisConnectException extends RuntimeException{
    public RedisConnectException(String message) {
        super(message);
    }

    public RedisConnectException(String message, Throwable cause) {
        super(message, cause);
    }
}
