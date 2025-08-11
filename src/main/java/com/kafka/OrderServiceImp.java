package com.kafka;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class OrderServiceImp implements  OrderService{
    private static final Logger logger = LoggerFactory.getLogger(OrderServiceImp.class);
    KafkaTemplate<String,Object> kafkaTemplate ;
    OrderServiceImp(KafkaTemplate<String,Object> kafkaTemplate){
        this.kafkaTemplate=kafkaTemplate;
    }

    @Transactional("kafkaTransactionManager")
    @Override
    public ResponseEntity<Object> placeOrder(Product product) throws ExecutionException, InterruptedException {
        String productId = UUID.randomUUID().toString();
        ProductMessage productMessage = new ProductMessage(productId,product.getName(),product.getPrice(),product.getQuantity());
       logger.info("*** Before calling kafka broker");

       ProducerRecord<String, Object> record = new ProducerRecord("places-order-event", productId, productMessage);
       record.headers().add("messageId",productId.toString().getBytes());
       SendResult<String,Object> sendResult = kafkaTemplate.send(record).get();

       if(true)throw  new RuntimeException("error happened");

//       future.whenComplete((success,exception)->{
//         if(success!=null){
//             logger.info("**** order confirm sent to queue");
//         }else{
//             logger.info("**** Exception occured ",exception.getMessage());
//         }
//       });

        logger.info("*** Successfully sent to kafka");
        return ResponseEntity.status(HttpStatus.OK).body(productId);
    }
    @KafkaListener(topics = "places-order-event", containerFactory = "listenerContainerFactory")
    private void eventkaListener(ProductMessage productMessage) throws RetryableException {
        if(productMessage.getQuantity()<0){
            throw  new NotRetryableException("quantity is negative");
        }else if(productMessage.getQuantity()>100){
            throw  new RetryableException("quatity must be less than 100");
        }
        logger.info("*********** event : {}",productMessage.getName());
    }

//    @KafkaListener(topics = "places-order-event", containerFactory = "listenerContainerFactory")
//    private void eventkaListener2(ProductMessage productMessage){
//        logger.info("*********** 2 ** event : {}",productMessage.getName());
//    }
//
//    @KafkaListener(topics = "places-order-event", containerFactory = "listenerContainerFactory")
//    private void eventkaListener3(ProductMessage productMessage){
//        logger.info("*********** 3 ** event : {}",productMessage.getName());
//    }
//
//    @KafkaListener(topics = "places-order-event", containerFactory = "listenerContainerFactory")
//    private void eventkaListener4(ProductMessage productMessage){
//        logger.info("*********** 4 ** event : {}",productMessage.getName());
//    }

    @KafkaListener(topics = "places-order-event-dlt", containerFactory = "listenerContainerFactory")
    private void eventkaListenerdlt(ProductMessage productMessage){
        logger.info("*********** dlt ** event : {}",productMessage.getName());
    }




}
