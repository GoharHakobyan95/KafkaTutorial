package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I am a kafka Consumer.");

        String groupId = "my_java_application";
        String topic = "demo_java";

        //create Producer properties
        Properties properties = new Properties();

        //connect to Localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //connect to Conduktor Playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2UBQl8NfjX0uyrH9rK7G9h\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyVUJRbDhOZmpYMHV5ckg5cks3RzloIiwib3JnYW5pemF0aW9uSWQiOjY5ODU2LCJ1c2VySWQiOjgwNzY4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI5ZGZmZGNmYy1hOThlLTRlYTktYmQyZi04ZTA4MTkwZWY2ODEifX0.cPeMAzNJf_VdzYfYxxNysQlzMeF4Rzo48VprP246eC8\";\n");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //set Consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());


        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create the Producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while (true) {
            logger.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

            }

        }

    }
}
