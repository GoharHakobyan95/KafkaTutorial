package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I am a kafka Producer.");

        //create Producer properties
        Properties properties = new Properties();

        //connect to Conductor Playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2UBQl8NfjX0uyrH9rK7G9h\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyVUJRbDhOZmpYMHV5ckg5cks3RzloIiwib3JnYW5pemF0aW9uSWQiOjY5ODU2LCJ1c2VySWQiOjgwNzY4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI5ZGZmZGNmYy1hOThlLTRlYTktYmQyZi04ZTA4MTkwZWY2ODEifX0.cPeMAzNJf_VdzYfYxxNysQlzMeF4Rzo48VprP246eC8\";\n");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a Producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello word");

        //send data
        producer.send(producerRecord);

        //tell the Producer to send all data and block until done ...synchronous
        producer.flush();

        //flush and close the Producer
        producer.close();

    }
}
