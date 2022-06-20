package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Enumeration;
import utils.ValQ1;

import java.time.Duration;

public class KafkaConnectorDemo {

    /*
     * Before running this program, load data in Kafka using kafka-producer.
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-broker:9092")
                .setTopics("flink-events")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> src = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<Tuple2<Long, ValQ1>> test = src.map(new MapFunction<String, Tuple2<Long, ValQ1>>() {
                    @Override
                    public Tuple2<Long, ValQ1> map(String s) throws Exception {
                        ValQ1 val = ValQ1.create(s);
                        return Tuple2.of(val.getSensor_id(), val);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, ValQ1>>forMonotonousTimestamps()
                        .withTimestampAssigner((tuple, l) -> tuple.f1.getTimestamp().getTime())
                        .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(0);
        test.print();

        env.execute("Kafka Connector Demo");
    }
}
