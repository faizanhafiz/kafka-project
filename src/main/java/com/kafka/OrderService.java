package com.kafka;

import org.springframework.http.ResponseEntity;

import java.util.concurrent.ExecutionException;

public interface OrderService {

     ResponseEntity<Object> placeOrder(Product product) throws ExecutionException, InterruptedException;
}
