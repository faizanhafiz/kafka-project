package com.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;



@RestController
@RequestMapping("/products")
public class ProductController {
    Logger logger = LoggerFactory.getLogger(ProductController.class);
    OrderService orderService;
    ProductController(OrderService orderService){
        this.orderService=orderService;
    }
    @PostMapping("addproduct")
    public ResponseEntity<Object> addProduct(@RequestBody Product product) throws RetryableException {
       try{

           ResponseEntity<Object> response  = orderService.placeOrder(product);
           return  response;
       }
       catch (Exception ex){
            logger.error("Error :",ex.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error happened"+ex.getMessage());
       }



    }
}
