package com.kafka;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class OrderServiceImp implements  OrderService{
    private static final Logger logger = LoggerFactory.getLogger(OrderServiceImp.class);
    KafkaTemplate<String,ProductMessage> kafkaTemplate ;
    OrderServiceImp(KafkaTemplate<String,ProductMessage> kafkaTemplate){
        this.kafkaTemplate=kafkaTemplate;
    }
    @Override
    public ResponseEntity<Object> placeOrder(Product product) throws ExecutionException, InterruptedException {
        String productId = UUID.randomUUID().toString();
        ProductMessage productMessage = new ProductMessage(productId,product.getName(),product.getPrice(),product.getQuantity());
       logger.info("*** Before calling kafka broker");
       SendResult<String,ProductMessage> sendResult = kafkaTemplate.send("places-order-event","1234567",productMessage).get();

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
    private void eventkaListener(ProductMessage productMessage){
        logger.info("*********** event : {}",productMessage.getName());
    }
}
