package kafka;

import kafka.queries.Query1;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import kafka.queries.Query;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Timestamp;
import java.util.*;

import utils.Event;
import utils.serdes.CustomSerdes;

public class Main {
    private static KafkaConsumer<Integer, Event> consumer;
    private static Query query;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "consumer-" + new Timestamp(System.currentTimeMillis()));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.Integer().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, Event> src =
                builder.stream("flink-events",
                        Consumed.with(Serdes.Integer(), CustomSerdes.Event()));

        Query q1 = new Query1(src, builder, props);

        switch (args[0]) {
            case ("Q1"):
                query = q1;
                break;
            case ("Q2"):
                //query = q2;
                break;
        }
        query.execute();
    }
}