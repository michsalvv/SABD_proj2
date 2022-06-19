package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
                .map(new MapFunction<String, Message>() {
                    @Override
                    public Message map(String value) throws Exception {
                        return Message.create(value);
                    }
                })
                .map((MapFunction<Message, Tuple3<Integer, Long, Float>>) value -> Tuple3.of(1, value.getTimestamp(), value.getRandom()))
                    .returns(Types.TUPLE(Types.INT, Types.LONG, Types.FLOAT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                        .<Tuple3<Integer, Long, Float>>forMonotonousTimestamps()
                            // forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((tuple, timestamp) -> tuple.f1)
                        .withIdleness(Duration.ofSeconds(10))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(0);
        dataStream.print();

        env.execute("Kafka Connector Demo");
    }


    public static class Message{
        public int sequence;
        public long timestamp;
        public String message;
        public float random;

        public static Message create(String rawMessage){
            var values = rawMessage.split(";");
            return new Message(Integer.parseInt(values[0]),
                    Long.parseLong(values[1]), values[2],
                    Float.parseFloat(values[3].replace(",",".")));
        }

        private Message(int sequence, long timestamp, String message, float random){
            this.sequence = sequence;
            this.timestamp = timestamp;
            this.message = message;
            this.random = random;
        }

        public float getRandom() {
            return random;
        }
        public int getSequence() {
            return sequence;
        }
        public long getTimestamp() {
            return timestamp;
        }
        public String getMessage() {
            return message;
        }
    }
}
