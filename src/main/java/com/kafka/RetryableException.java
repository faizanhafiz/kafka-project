package com.kafka;

public class RetryableException extends  Exception{
    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }
}
