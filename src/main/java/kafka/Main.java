package kafka;

import kafka.queries.Query1;
import kafka.queries.Query2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import kafka.queries.Query;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Timestamp;
import java.util.*;

import utils.tuples.Event;
import utils.serdes.CustomSerdes;

public class Main {
    private static KafkaConsumer<Integer, Event> consumer;
    private static Query query;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "consumer-" + new Timestamp(System.currentTimeMillis()));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker:9092");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 180 * 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 30 * 1024 * 1024L);       // x MB di caching
        props.put("metrics.recording.level", "DEBUG");
        props.put("metric.reporters", "org.apache.kafka.common.metrics.JmxReporter");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, Event> src =
                builder.stream("flink-events",
                        Consumed.with(Serdes.Integer(), CustomSerdes.Event()));

        Query q1 = new Query1(src, builder, props);
        Query q2 = new Query2(src, builder, props);

        switch (args[0]) {
            case ("Q1"):
                query = q1;
                break;
            case ("Q2"):
                query = q2;
                break;
        }
        query.execute();
    }
}