package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCB {

    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCB.class);
    public static void main(String[] args) throws InterruptedException {

        final String topicName = "multipart-topic";
        // kafkaProducer configuration
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.238.137:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for(int seq = 0 ; seq < 20 ; seq++){
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world " + seq);
            kafkaProducer.send(producerRecord, new CustomCallback(seq));
        }

        Thread.sleep(1000l);

        kafkaProducer.close();

    }
}
