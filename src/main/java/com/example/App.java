package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "input-topic";
        String username = args.length > 2 ? args[2] : null;
        String password = args.length > 3 ? args[3] : null;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        var sourceBuilder = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-console-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema());

        if (username != null && password != null) {
            sourceBuilder.setProperty("security.protocol", "SASL_PLAINTEXT");
            sourceBuilder.setProperty("sasl.mechanism", "SCRAM-SHA-256");
            sourceBuilder.setProperty("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required "
                            + "username=\"" + username + "\" "
                            + "password=\"" + password + "\";");
        }

        KafkaSource<String> source = sourceBuilder.build();

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        stream.print();

        env.execute("Flink Kafka Consumer");
    }
}
