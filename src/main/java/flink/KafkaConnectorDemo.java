package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.ValQ1;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class KafkaConnectorDemo {
    //TODO Fare un serializzatore vero
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
        var src = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        var dataStream = src
                .map(values -> Tuple2.of(ValQ1.create(values), 1))
                .returns(Types.TUPLE(Types.GENERIC(ValQ1.class), Types.INT))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<ValQ1, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(1))                          // Assumiamo il dataset ordinato
                        .withTimestampAssigner((tuple, timestamp) -> tuple.f0.getTimestamp().getTime())
                        .withIdleness(Duration.ofMinutes(1))
                        )
                .keyBy(values -> values.f0.getSensor_id())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .aggregate(new Average());
        dataStream.print();
        env.execute("Kafka Connector Demo");
    }
}
