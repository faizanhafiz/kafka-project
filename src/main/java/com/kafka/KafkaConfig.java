package com.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

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
        config.put(ProducerConfig.ACKS_CONFIG,"all");
        config.put(ProducerConfig.RETRIES_CONFIG,retries);
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,backoff_ms);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,timeout);
        config.put(ProducerConfig.LINGER_MS_CONFIG,linger);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,requestTimeout);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"trans-1");

        return config;
    }


    @Bean
    ProducerFactory<String ,Object> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }
    @Bean
    KafkaTemplate<String ,Object> kafkaTemplate(ProducerFactory<String ,Object> producerFactory){
        return  new KafkaTemplate<>(producerFactory);
    }
    @Bean
    KafkaTransactionManager<String,Object> kafkaTransactionManager( ProducerFactory<String ,Object> producerFactory){
        return  new KafkaTransactionManager<>(producerFactory);
    }


   //Consumer config
    @Bean
    ConsumerFactory<String,Object > consumerFactory(){
        Map<String , Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,JsonDeserializer.class);
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,JsonDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,"my-consumer-group");
        config.put(JsonDeserializer.TRUSTED_PACKAGES,"*");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String , Object> listenerContainerFactory(ConsumerFactory<String ,Object> consumerFactory,KafkaTemplate<String ,Object> kafkaTemplate){
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(500,3));
        errorHandler.addRetryableExceptions( RetryableException.class);
        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
     ConcurrentKafkaListenerContainerFactory<String,Object> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
     listenerContainerFactory.setConsumerFactory(consumerFactory);
     listenerContainerFactory.setCommonErrorHandler(errorHandler);
     return listenerContainerFactory;

    }


}
