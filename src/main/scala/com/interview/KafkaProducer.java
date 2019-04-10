package com.interview;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", "macbookpro:9092");

        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);

        String topic = "pkoffset";
        for (int index = 0; index < 100; index++) {
            producer.send(new KeyedMessage<String, String>(topic, index + "", index + "ruozeshuju:" + UUID.randomUUID()));
        }

        System.out.println("Kafka生产者生产数据完毕...");

    }

}
