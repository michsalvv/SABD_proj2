package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import queries.flink.Query1;
import queries.flink.Query2;

import java.time.Duration;

public class Main {
    //TODO Fare un serializzatore vero
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers("kafka-broker:9092")
                .setTopics("flink-events")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setUnbounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new CustomDeserializer())
                .build();

        // BIBBIA
        var src = env.fromSource(source, WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, l) -> event.getTimestamp().getTime()),
                "Kafka Source");

        var q1 = new Query1(env,src);
        var q2 = new Query2(env, src);
//        q1.execute();
        q2.execute();

    }
}
