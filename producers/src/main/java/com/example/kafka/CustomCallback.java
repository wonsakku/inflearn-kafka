package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    public static final Logger logger = LoggerFactory.getLogger(CustomCallback.class);
    private final Integer seq;

    public CustomCallback(Integer seq) {
        this.seq = seq;
    }


    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            logger.info("seq : {} partition : {} offset : {} timestamp : {}",
                    this.seq, metadata.partition(), metadata.offset(), metadata.timestamp());
        }else{
            logger.error("exception error from broker " + exception.getMessage());
        }
    }
}
