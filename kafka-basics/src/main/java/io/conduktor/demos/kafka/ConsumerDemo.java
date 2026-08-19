package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        LOG.info("Initiating Kafka Consumer!");

        final var groupId = "my-java-application";
        final var topic = "demo_java";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        //none/earliest (from-beginning)/latest (from now on)
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(List.of(topic));

        //poll data
        while(true){
            LOG.info("Polling");

            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (final ConsumerRecord<String, String> record : records) {
                LOG.info("""
                        New Record!
                        Key: {}
                        Value: {}
                        Partition: {}
                        Offset: {}
                        """, record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}
