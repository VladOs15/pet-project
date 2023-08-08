package com.example.petproject.Exception;

public class KafkaConnectionException extends RuntimeException{
    public KafkaConnectionException(String message) {
        super(message);
    }

    public KafkaConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
