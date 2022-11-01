package io.conductor.demos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        LOGGER.info("Hello World");

        //create Produced Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //create a  producer data
        ProducerRecord<String,String> producerRecord = new ProducerRecord("demo_java","Hello World");

        //send data - asynchronous
        kafkaProducer.send(producerRecord);

        //flush and close the producer

        kafkaProducer.flush();

        //flush and close Producer
        kafkaProducer.close();
    }
}
