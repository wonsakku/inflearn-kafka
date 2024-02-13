package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerAsync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class);
    public static void main(String[] args) throws InterruptedException {

        final String topicName = "simple-topic";
        // kafkaProducer configuration
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.238.137:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world 44");

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if(exception == null){
                logger.info("\n ############### record metadata received ############## \n" +
                        "partition : " + metadata.partition() + "\n" +
                        "offset : " + metadata.offset()  + "\n" +
                        "timestamp : " + metadata.timestamp()  + "\n"
                );
            }else{
                logger.error("exception error from broker " + exception.getMessage());
            }
        });

        Thread.sleep(1000l);

        kafkaProducer.close();

    }
}
