//package com.example.projectxxx.Kafka.Config;
//import org.apache.kafka.clients.admin.NewTopic;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.TopicBuilder;
//
//@Configuration
//public class KafkaTopicConfig {
//    @Bean
//    public NewTopic Topic(){
//        return TopicBuilder
//                .name("stock")
//                .partitions(2)
//                .replicas(2)
//                .build();
//    }
//}