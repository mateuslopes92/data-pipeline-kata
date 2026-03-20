package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

import static spark.Spark.*;

public class ApiIngestionApp {

    public static void main(String[] args) {

        port(8082);
        init();

        Properties props = new Properties();
        props.put(
            "bootstrap.servers",
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092")
        );
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ObjectMapper mapper = new ObjectMapper();

        post("/sales", (req, res) -> {

            try {
                String event = req.body();

                // Parse JSON
                JsonNode node = mapper.readTree(event);

                // Extract city as key
                String key = node.has("city") ? node.get("city").asText() : "unknown";

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("sales-api", key, event);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });

                System.out.println("Received and sent to Kafka: " + event);

                res.status(200);
                return "Event sent to Kafka!";

            } catch (Exception e) {
                res.status(400);
                return "Invalid JSON";
            }
        });

        awaitInitialization();

        System.out.println("API Ingestion Service is running on port 8082...");
    }
}