package com.pipeline;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

import static spark.Spark.*;

public class ApiIngestionApp {

    public static void main(String[] args) {

        port(8080);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        post("/sales", (req, res) -> {

            String event = req.body();

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("sales-api", null, event);

            producer.send(record);

            System.out.println("Received and sent to Kafka: " + event);

            return "Event sent to Kafka!";
        });
    }
}