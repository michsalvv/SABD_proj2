package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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

        var src = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        var dataStream = src
                .map(value -> Tuple2.of(ValQ1.create(value), 1))
                .returns(Types.TUPLE(Types.GENERIC(ValQ1.class), Types.INT))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        //dataStream.print();

        SingleOutputStreamOperator<Tuple2<Long, Message>> outputStream = src.map(new MapFunction<String, Tuple2<Long, Message>>() {
                    @Override
                    public Tuple2<Long, Message> map(String s) throws Exception {
                        Message m = Message.create(s);

                        return Tuple2.of(m.getTimestamp(), m);
                    }
                })
                        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                                .sum(0);
        outputStream.print();

        env.execute("Kafka Connector Demo");
    }
}
