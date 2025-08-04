package com.kafka;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.kafka.producer.retries}")
    private String retries;

    @Value("${spring.kafka.producer.properties.retry.backoff.ms}")
    private String backoff_ms;

    @Value("${spring.kafka.producer.properties.delivery.timeout.ms}")
    private String timeout;

    @Value("${spring.kafka.producer.properties.linger}")
    private String linger;
    @Value("${spring.kafka.producer.properties.request.timeout.ms}")
    private String requestTimeout;

    @Bean
    NewTopic createTopic(){
       return TopicBuilder.name("places-order-event")
                .replicas(3)
                .partitions(3)
                .build();
    }



    Map<String ,Object> producerConfig(){
        HashMap<String,Object> config  = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        config.put(ProducerConfig.ACKS_CONFIG,"1");
        config.put(ProducerConfig.RETRIES_CONFIG,retries);
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,backoff_ms);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,timeout);
        config.put(ProducerConfig.LINGER_MS_CONFIG,linger);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,requestTimeout);
        return config;
    }

    @Bean
    ProducerFactory<String ,ProductMessage> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }
    @Bean
    KafkaTemplate<String ,ProductMessage> kafkaTemplate(){
        return  new KafkaTemplate<>(producerFactory());
    }




}
