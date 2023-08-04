package com.example.petproject.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

public class KafkaTopicConfig {

    @Bean
    public NewTopic topic(){
        return TopicBuilder.name("kafka_topic")
                .build();
    }

    @Bean
    public NewTopic topicJson(){
        return TopicBuilder.name("kafka_topic_json")
                .build();
    }

    @Bean
    public NewTopic topicError(){
        return TopicBuilder.name("error-topic")
                .build();
    }
}
