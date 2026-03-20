package com.pipeline;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.io.InputStreamReader;

public class FileIngestionApp {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(
            "bootstrap.servers",
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092")
        );
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                FileIngestionApp.class
                    .getClassLoader()
                    .getResourceAsStream("sales-event.ndjson")
            )
        );

        StringBuilder jsonBuilder = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {

            jsonBuilder.append(line.trim());

            // Detect end of JSON object
            if (line.trim().endsWith("}")) {

                String json = jsonBuilder.toString();

                String key = "unknown";

                try {
                    int cityStart = json.indexOf("\"city\":\"") + 8;
                    int cityEnd = json.indexOf("\"", cityStart);
                    key = json.substring(cityStart, cityEnd);
                } catch (Exception e) {
                    // fallback stays "unknown"
                }

                producer.send(new ProducerRecord<>(
                        "sales-file",
                        key,
                        json
                ));

                System.out.println("Sent: " + json);

                // Reset for next JSON object
                jsonBuilder.setLength(0);
            }
        }

        reader.close();

        producer.flush();
        producer.close();
    }
}