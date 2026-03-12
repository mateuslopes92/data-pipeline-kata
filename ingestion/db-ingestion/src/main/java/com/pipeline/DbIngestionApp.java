package com.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;

import java.sql.*;
import java.util.Properties;

public class DbIngestionApp {

    public static void main(String[] args) throws Exception {

        String jdbcUrl = "jdbc:postgresql://localhost:5433/sales_source";
        String user = "postgres";
        String password = "postgres";

        Connection connection = DriverManager.getConnection(jdbcUrl, user, password);

        String query = "SELECT * FROM sales";
        PreparedStatement statement = connection.prepareStatement(query);

        ResultSet rs = statement.executeQuery();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ObjectMapper mapper = new ObjectMapper();

        while (rs.next()) {

            SalesEvent event = new SalesEvent();
            event.id = rs.getString("id");
            event.city = rs.getString("city");
            event.salesman = rs.getString("salesman");
            event.amount = rs.getDouble("amount");
            event.source = rs.getString("source");
            event.timestamp = rs.getLong("timestamp");

            String json = mapper.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("sales-db", event.id, json);

            producer.send(record);

            System.out.println("Sent: " + json);
        }

        producer.flush();
        producer.close();

        connection.close();
    }
}