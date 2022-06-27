package sparkStreaming;

import sparkStreaming.queries.Query;
import sparkStreaming.queries.Query1;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import utils.Event;
import utils.serdes.EventDeserializer;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    private static Query query;

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[1]");
        sparkConf.setAppName("Spark Streaming");
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka-broker:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", EventDeserializer.class);
        kafkaParams.put("group.id", "id");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = List.of("flink-events");

        JavaInputDStream<ConsumerRecord<String, Event>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Event>Subscribe(topics, kafkaParams));

        Query q1 = new Query1(streamingContext, stream);

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
