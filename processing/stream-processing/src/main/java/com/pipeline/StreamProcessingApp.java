package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class StreamProcessingApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-processing");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        KStream<String, String> dbStream = builder.stream("sales-db");
        KStream<String, String> fileStream = builder.stream("sales-file");
        KStream<String, String> apiStream = builder.stream("sales-api");

        KStream<String, String> mergedStream =
                dbStream.merge(fileStream).merge(apiStream);

        KTable<String, Integer> salesByCity =
                mergedStream
                        .map((key, value) -> {
                            try {

                                JsonNode node = mapper.readTree(value);

                                String city = node.get("city").asText();
                                int amount = node.get("amount").asInt();

                                return KeyValue.pair(city, amount);

                            } catch (Exception e) {
                                return KeyValue.pair("unknown", 0);
                            }
                        })
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                        .reduce(Integer::sum);

                        salesByCity
                        .toStream()
                        .peek((city, total) ->
                                System.out.println("TOTAL SALES -> " + city + " = " + total)
                        )
                        .mapValues(total -> String.valueOf(total))
                        .to("sales-by-city", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}