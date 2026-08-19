package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        LOG.info("Initiating Kafka Consumer!");

        final var groupId = "my-java-application";
        final var topic = "demo_java";

        //create consumer properties
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        //none/earliest (from-beginning)/latest (from now on)
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                LOG.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                //join the mainThread to allow the execution  of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
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

        } catch (WakeupException e){
            LOG.info("Consumer is starting to shutdown...");
        } catch (Exception e){
            LOG.error("Unexpected exception in the consumer", e);
        } finally {
            //close the consumer, this will also commit the offsets
            consumer.close();
            LOG.info("The consumer is now gracefully shutdown");
        }
    }
}
