package com.pipeline;

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

        KStream<String, String> dbStream =
                builder.stream("sales-db");

        KStream<String, String> fileStream =
                builder.stream("sales-file");

        KStream<String, String> apiStream =
                builder.stream("sales-api");

        KStream<String, String> mergedStream =
                dbStream.merge(fileStream).merge(apiStream);

        mergedStream.peek((key, value) ->
                System.out.println("Processing: " + value)
        );

        mergedStream.to("sales-processed");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}