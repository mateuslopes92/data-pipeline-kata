package com.pipeline;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class FileIngestionApp {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        BufferedReader reader = new BufferedReader(
                new FileReader("data/sales-event.ndjson")
        );

        String line;

        while ((line = reader.readLine()) != null) {

            producer.send(new ProducerRecord<>(
                    "sales-file",
                    line
            ));

            System.out.println("Sent: " + line);
        }

        reader.close();

        producer.flush();
        producer.close();
    }
}