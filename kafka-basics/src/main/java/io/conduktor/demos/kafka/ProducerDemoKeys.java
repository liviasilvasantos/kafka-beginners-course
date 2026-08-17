package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        LOG.info("Initiating Kafka Producer With Callback!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                final var topic = "demo_java";
                final var key = "id_" + i;
                final var value = "Hello, there, " + i + " !";
                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            //success
                            LOG.info("""
                                            Received new metadata
                                            Key: {}
                                            Topic: {}
                                            Partition: {}
                                            Offset: {}
                                            Timestamp: {}
                                            """, key,
                                    recordMetadata.topic(), recordMetadata.partition(),
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
