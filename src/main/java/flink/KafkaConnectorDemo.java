package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

        SingleOutputStreamOperator<Double> test = src.map((MapFunction<String, ValQ1>) s -> {
            return ValQ1.create(s);
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ValQ1>forMonotonousTimestamps()
                        .withTimestampAssigner((val, l) -> val.getTimestamp().getTime())
//                        .withIdleness(Duration.ofMillis(1))
                )
                .keyBy(val -> val.getSensor_id())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregatorQuery1());

        test.print();

        env.execute("Kafka Connector Demo");
    }
}
