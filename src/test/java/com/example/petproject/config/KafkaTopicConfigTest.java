package com.example.petproject.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class KafkaTopicConfigTest {

    @Autowired
    private KafkaTopicConfig kafkaTopicConfig;

    private static final String TOPIC_NAME = "kafka_topic";
    private static final String TOPIC_JSON_NAME = "kafka_topic_json";
    private static final String TOPIC_ERROR_NAME = "error-topic";
    private static final String TOPIC_TEST_NAME = "test-topic";

    @Test
    public void testCreationTopic() {
        assertNotNull(kafkaTopicConfig.topic());
        assertNotNull(kafkaTopicConfig.topicJson());
        assertNotNull(kafkaTopicConfig.topicError());
        assertNotNull(kafkaTopicConfig.topicTest());
    }

    @Test
    public void testTopicNames(){
        assertEquals(TOPIC_NAME, kafkaTopicConfig.topic().name());
        assertEquals(TOPIC_JSON_NAME, kafkaTopicConfig.topicJson().name());
        assertEquals(TOPIC_ERROR_NAME, kafkaTopicConfig.topicError().name());
        assertEquals(TOPIC_TEST_NAME, kafkaTopicConfig.topicTest().name());
    }

    @Test
    public void testKafkaInteraction() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(adminProps);

        assertTrue(adminClient.listTopics().names().get().contains(TOPIC_NAME));
        assertTrue(adminClient.listTopics().names().get().contains(TOPIC_JSON_NAME));
        assertTrue(adminClient.listTopics().names().get().contains(TOPIC_ERROR_NAME));
        assertTrue(adminClient.listTopics().names().get().contains(TOPIC_TEST_NAME));
    }

    @Test
    public void testTopicBeansNotNull() {
        assertNotNull(kafkaTopicConfig.topic());
        assertNotNull(kafkaTopicConfig.topicJson());
        assertNotNull(kafkaTopicConfig.topicError());
        assertNotNull(kafkaTopicConfig.topicTest());
    }
}
