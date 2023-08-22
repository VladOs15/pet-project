//package com.example.petproject.controller;
//
//import com.example.petproject.kafka.RedisKafkaProducer;
//import com.example.petproject.payload.User;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.kafka.core.KafkaTemplate;
//import redis.clients.jedis.Jedis;
//
//import static org.junit.jupiter.api.Assertions.*;
//import static org.mockito.Mockito.*;
//
//public class RedisMessageControllerTest {
//
//    @Mock
//    private RedisKafkaProducer kafkaProducer;
//
//    @Mock
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    @InjectMocks
//    private RedisMessageController messageController;
//
//    // ...
//    @BeforeEach
//    public void setUp() {
//        MockitoAnnotations.initMocks(this);
//        messageController = new RedisMessageController(kafkaProducer);
//    }
//
//    @Test
//    public void testPublishToRedis_NullName() {
//        User user = new User();
//        user.setId(1L);
//        user.setAge(25);
//        user.setWork("Developer");
//
//        ResponseEntity<String> response = messageController.publishToRedis(user);
//
//        assertEquals(ResponseEntity.badRequest().body("Имя пользователя не указано"), response);
//    }
//
//    @Test
//    public void testIsRedisConnected_Success() {
//        try (Jedis jedis = mock(Jedis.class)) {
//            when(jedis.ping()).thenReturn("PONG");
//
//            boolean isConnected = messageController.isRedisConnected();
//
//            assertTrue(isConnected);
//        }
//    }
//
//    @Test
//    public void testIsRedisConnected_Error() {
//        try (Jedis jedis = mock(Jedis.class)) {
//            when(jedis.ping()).thenReturn("INVALID_RESPONSE");
//
//            boolean isConnected = messageController.isRedisConnected();
//
//            assertFalse(isConnected);
//        }
//    }
//
////    @Test
////    public void testIsKafkaConnected_Success() {
////        // Mock KafkaProducer and AdminClient
////        KafkaProducer<String, String> kafkaProducerMock = mock(KafkaProducer.class);
////        AdminClient adminClientMock = mock(AdminClient.class);
////        when(kafkaProducerMock.close()).thenReturn(null);
////        when(adminClientMock.listTopics()).thenReturn(null);
////
////        boolean isConnected = messageController.isKafkaConnected();
////
////        assertTrue(isConnected);
////    }
////
////    @Test
////    public void testIsKafkaConnected_Error() {
////        // Mock KafkaProducer with exception
////        KafkaProducer<String, String> kafkaProducerMock = mock(KafkaProducer.class);
////        when(kafkaProducerMock.close()).thenThrow(new RuntimeException("Error closing KafkaProducer"));
////
////        boolean isConnected = messageController.isKafkaConnected();
////
////        assertFalse(isConnected);
////    }
////
////    @Test
////    public void testIsKafkaConnected_TopicNotFound() {
////        // Mock KafkaProducer and AdminClient with no topics
////        KafkaProducer<String, String> kafkaProducerMock = mock(KafkaProducer.class);
////        AdminClient adminClientMock = mock(AdminClient.class);
////        when(kafkaProducerMock.close()).thenReturn(null);
////        when(adminClientMock.listTopics()).thenReturn(null);
////
////        boolean isConnected = messageController.isKafkaConnected();
////
////        assertFalse(isConnected);
////    }
//
//    @Test
//    public void testHandleKafkaError_Success() {
//        User user = new User();
//        user.setId(1L);
//        user.setName("John");
//
//        when(kafkaTemplate.send(eq("kafka_topic_json"), anyString())).thenReturn(null);
//
//        ResponseEntity<String> response = messageController.handleKafkaError(user);
//
//        assertEquals(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка при отправке в Kafka, сообщение отправлено в дополнительный топик"), response);
//    }
//
//    @Test
//    public void testHandleKafkaError_Error() {
//        User user = new User();
//        user.setId(1L);
//        user.setName("John");
//
//        when(kafkaTemplate.send(anyString(), anyString())).thenThrow(new RuntimeException("Error sending to Kafka"));
//
//        ResponseEntity<String> response = messageController.handleKafkaError(user);
//
//        assertEquals(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Произошла ошибка при отправке и в дополнительный топик"), response);
//    }
//
//    // ...
//}