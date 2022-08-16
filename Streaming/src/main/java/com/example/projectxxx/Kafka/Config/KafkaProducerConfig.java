//package com.example.projectxxx.Kafka.Config;
//
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.core.DefaultKafkaProducerFactory;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.core.ProducerFactory;
//
//import java.util.HashMap;
//import java.util.Map;
//
//@Configuration
//public class KafkaProducerConfig {
//    private static final String bootstrap_sever = "localhost:9092";
//    public Map<String, Object> producerConfig() {
//        Map<String, Object> pros = new HashMap<>();
//        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_sever);
//        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        return pros;
//    }
//
//    @Bean
//    public ProducerFactory<String, String> producerFactory(){
//        return new DefaultKafkaProducerFactory<>(this.producerConfig());
//    }
//
//    @Bean
//    public KafkaTemplate<String, String> kafkaTemplate(){
//        return new KafkaTemplate<>(this.producerFactory());
//    }
//}
//
