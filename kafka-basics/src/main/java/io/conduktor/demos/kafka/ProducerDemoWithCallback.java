package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        LOG.info("Initiating Kafka Producer With Callback!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400");

        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {
                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello, there, callbackers " + i + " !");

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            //success
                            LOG.info("""
                                            Received new metadata\n
                                            Topic: {}\n
                                            Partition: {}\n
                                            Offset: {}\n
                                            Timestamp: {}
                                            """, recordMetadata.topic(), recordMetadata.partition(),
                                    recordMetadata.offset(), recordMetadata.timestamp());
                        } else {
                            //error
                            LOG.error("Error ao produzir mensagem", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //flush and close the producer
        producer.flush();
        producer.close();
    }
}
